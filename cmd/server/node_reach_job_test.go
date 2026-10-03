package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// holdReachJobs makes every detached reach computation block until the
// returned release func is called, and counts how many started.
func holdReachJobs(t *testing.T) (release func(), started *atomic.Int32) {
	t.Helper()
	gate := make(chan struct{})
	var n atomic.Int32
	reachJobStartHook = func() { n.Add(1); <-gate }
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { release(); reachJobStartHook = nil })
	return release, &n
}

func shortReachWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := reachWaitBudget
	reachWaitBudget = d
	t.Cleanup(func() { reachWaitBudget = old })
}

func reachTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	resetReachState(t)
	db, n := newReachIntegrationDB(t, `["AABB","01FA","CCDD"]`)
	t.Cleanup(func() { db.conn.Close() })
	cfg := &Config{}
	return &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}, n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A computation slower than the wait budget answers 202 "building" instead of
// being cut off, keeps running, and the next request gets the finished result.
func TestNodeReach_SlowComputeAnswersBuildingThenCaches(t *testing.T) {
	shortReachWait(t, 50*time.Millisecond)
	release, started := holdReachJobs(t)
	srv, n := reachTestServer(t)

	rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("slow compute: status=%d want 202 (body=%s)", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Retry-After"); got != strconv.Itoa(reachRetryAfterSeconds) {
		t.Errorf("Retry-After=%q want %d", got, reachRetryAfterSeconds)
	}
	var body struct {
		Status string `json:"status"`
		Retry  int    `json:"retry_after_seconds"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body.Status != "building" || body.Retry != reachRetryAfterSeconds {
		t.Fatalf("bad building body %q (%v)", rr.Body.String(), err)
	}

	release() // let the job finish in the background
	waitFor(t, "finished result to be cached", func() bool {
		_, ok := srv.reachCacheGet(n + "|30|g" + strconv.FormatUint(srv.cfg.BlacklistGeneration(), 10))
		return ok
	})
	rr2 := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr2.Code != http.StatusOK {
		t.Fatalf("after job finished: status=%d want 200", rr2.Code)
	}
	var resp NodeReachResponse
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp); err != nil || resp.Importance.RelayObservations < 1 {
		t.Fatalf("cached result wrong: %v %+v", err, resp.Importance)
	}
	if started.Load() != 1 {
		t.Fatalf("computation ran %d times, want exactly 1", started.Load())
	}
}

// Many callers asking for the same node while it is computing share ONE job.
func TestNodeReach_ConcurrentCallersShareOneJob(t *testing.T) {
	shortReachWait(t, 50*time.Millisecond)
	release, started := holdReachJobs(t)
	srv, n := reachTestServer(t)

	var wg sync.WaitGroup
	codes := make([]int, 8)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = serveReach(srv, "/api/nodes/"+n+"/reach?days=30").Code
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusAccepted {
			t.Errorf("caller %d: status=%d want 202", i, c)
		}
	}
	release()
	waitFor(t, "job to finish", func() bool {
		srv.reach.jobsMu.Lock()
		defer srv.reach.jobsMu.Unlock()
		return len(srv.reach.jobs) == 0
	})
	if started.Load() != 1 {
		t.Fatalf("8 callers started %d computations, want 1", started.Load())
	}
}

// If the browser goes away mid-request the work still completes and is cached,
// so the user's retry (or the next visitor) gets it instantly.
func TestNodeReach_ClientDisconnectDoesNotCancelJob(t *testing.T) {
	shortReachWait(t, 5*time.Second) // long: only the disconnect ends the wait
	release, _ := holdReachJobs(t)
	srv, n := reachTestServer(t)

	router := mux.NewRouter()
	router.HandleFunc("/api/nodes/{pubkey}/reach", srv.handleNodeReach).Methods("GET")
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("GET", "/api/nodes/"+n+"/reach?days=30", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { router.ServeHTTP(httptest.NewRecorder(), req); close(done) }()

	waitFor(t, "job to start", func() bool {
		srv.reach.jobsMu.Lock()
		defer srv.reach.jobsMu.Unlock()
		return len(srv.reach.jobs) == 1
	})
	cancel() // client leaves
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the client disconnected")
	}
	release()
	waitFor(t, "job to complete after the client left", func() bool {
		_, ok := srv.reachCacheGet(n + "|30|g" + strconv.FormatUint(srv.cfg.BlacklistGeneration(), 10))
		return ok
	})
}

// A fast computation still answers 200 directly (no extra round trip).
func TestNodeReach_FastComputeAnswersDirectly(t *testing.T) {
	srv, n := reachTestServer(t)
	if rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30"); rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
}
