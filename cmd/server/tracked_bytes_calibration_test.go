package main

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"
)

// Prod-measured field shapes, sampled from the grav deployment on 2026-08-08
// (200k most recent observations, 50k most recent transmissions, 30 observers).
// These drive the calibration fixture so the estimator is tuned against the
// data shape it actually sees rather than an invented one.
const (
	calObsPerTx      = 25  // measured 24.64 observations per transmission
	calObserverCount = 30  // observers table row count
	calObserverIDLen = 64  // hex pubkey
	calObserverName  = 14  // avg length(name)
	calObserverIATA  = 3   // avg length(iata)
	calDirectionLen  = 2   // avg length(direction)
	calObsPathJSON   = 44  // avg length(path_json)
	calTimestampLen  = 24  // "2026-08-07T21:45:55.123Z"
	calTxRawHexLen   = 159 // avg length(raw_hex)
	calTxHashLen     = 16  // avg length(hash)
	calTxDecodedLen  = 268 // avg length(decoded_json)
	calTxFirstSeen   = 20  // avg length(first_seen)
	calTxHops        = 3   // typical path hop count
	calScorePresent  = 63  // percent of observations with a non-NULL score
)

// freshString returns a string with its own backing array, mirroring what the
// SQL driver produces per scanned row. Using a shared literal here would hide
// exactly the per-observation duplication the estimator needs to account for.
func freshString(n int, seed int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i+seed)%26)
	}
	return string(b)
}

// buildCalibrationStore populates a store the way the chunked loader does:
// same struct fields, same per-tx dedup maps, same secondary indexes. It
// deliberately does NOT go through SQLite — this measures the cost of the
// in-memory representation, which is what trackedBytes claims to model.
func buildCalibrationStore(nTx int) *PacketStore {
	s := NewPacketStore(nil, nil)

	observerIDs := make([]string, calObserverCount)
	for i := range observerIDs {
		observerIDs[i] = freshString(calObserverIDLen, i)
	}

	for i := 0; i < nTx; i++ {
		pt := 1
		tx := &StoreTx{
			ID:          i,
			RawHex:      freshString(calTxRawHexLen, i),
			Hash:        freshString(calTxHashLen, i),
			FirstSeen:   freshString(calTxFirstSeen, i),
			DecodedJSON: freshString(calTxDecodedLen, i),
			PathJSON:    freshString(calObsPathJSON, i),
			PayloadType: &pt,
			obsKeys:     make(map[string]bool),
			observerSet: make(map[string]bool),
		}
		hops := make([]string, calTxHops)
		for h := range hops {
			hops[h] = freshString(2, i+h)
		}
		tx.parsedPath = hops
		tx.pathParsed = true

		s.byHash[tx.Hash] = tx
		s.byTxID[tx.ID] = tx
		s.packets = append(s.packets, tx)
		s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
		addTxToSubpathIndexFull(s.spIndex, s.spTxIndex, tx)
		s.trackedBytes += estimateStoreTxBytes(tx)

		for j := 0; j < calObsPerTx; j++ {
			// Each observation carries its OWN copy of the observer strings,
			// exactly as the LEFT JOIN scan produces them (see #v3 schema:
			// observations store only observer_idx; these are re-inflated
			// per row at load time).
			obsID := freshString(calObserverIDLen, i*31+j)
			obsPJ := freshString(calObsPathJSON, i*17+j)
			snr := float64(j)
			rssi := float64(-j)
			obs := &StoreObs{
				ID:             i*calObsPerTx + j,
				TransmissionID: tx.ID,
				ObserverID:     obsID,
				ObserverName:   freshString(calObserverName, j),
				ObserverIATA:   freshString(calObserverIATA, j),
				Direction:      freshString(calDirectionLen, j),
				SNR:            &snr,
				RSSI:           &rssi,
				PathJSON:       obsPJ,
				Timestamp:      freshString(calTimestampLen, i*7+j),
			}
			if j*100/calObsPerTx < calScorePresent {
				sc := j
				obs.Score = &sc
			}

			dk := obsID + "|" + obsPJ
			tx.obsKeys[dk] = true
			if !tx.observerSet[obsID] {
				tx.observerSet[obsID] = true
				tx.UniqueObserverCount++
			}
			tx.Observations = append(tx.Observations, obs)
			tx.ObservationCount++

			s.byObsID[obs.ID] = obs
			s.byObserver[obsID] = append(s.byObserver[obsID], obs)
			s.totalObs++
			s.trackedBytes += estimateStoreObsBytes(obs)
		}
	}
	return s
}

// measureHeapDelta reports the live heap growth caused by fn's return value.
func measureHeapDelta(fn func() interface{}) (delta uint64, held interface{}) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	held = fn()

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	if after.HeapAlloc > before.HeapAlloc {
		delta = after.HeapAlloc - before.HeapAlloc
	}
	return delta, held
}

// TestTrackedBytesMatchesActualHeap is the Stage-2 regression guard: the
// store's self-accounting must stay within tolerance of the live heap it
// actually occupies. Before calibration this failed at roughly 0.3x — the
// eviction high-watermark therefore fired at ~3x the RAM the operator asked
// for via packetStore.maxMemoryMB, which is how prod reached ~4.8GB RSS
// against a 1GB-shaped budget and entered a GC death spiral.
//
// Tolerance is deliberately wide: this models allocator size classes and map
// bucket overhead, not an exact byte count. It only has to be good enough that
// maxMemoryMB means roughly what it says.
func TestTrackedBytesMatchesActualHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100MB; skipped under -short")
	}
	const nTx = 4000

	delta, held := measureHeapDelta(func() interface{} {
		return buildCalibrationStore(nTx)
	})
	store := held.(*PacketStore)
	tracked := uint64(store.trackedBytes)

	if delta == 0 {
		t.Fatal("measured zero heap growth — fixture did not allocate")
	}

	ratio := float64(tracked) / float64(delta)
	perObsTracked := float64(tracked) / float64(store.totalObs)
	perObsActual := float64(delta) / float64(store.totalObs)

	t.Logf("tx=%d obs=%d", len(store.packets), store.totalObs)
	t.Logf("tracked = %.1f MB (%.0f B/obs)", float64(tracked)/1048576, perObsTracked)
	t.Logf("actual  = %.1f MB (%.0f B/obs)", float64(delta)/1048576, perObsActual)
	t.Logf("ratio tracked/actual = %.2f", ratio)

	runtime.KeepAlive(store)

	if ratio < 0.75 || ratio > 1.25 {
		t.Errorf("trackedBytes is %.2fx actual heap (want 0.75-1.25). "+
			"tracked=%.1fMB actual=%.1fMB. maxMemoryMB would be off by this factor.",
			ratio, float64(tracked)/1048576, float64(delta)/1048576)
	}
}

// TestObsDedupEntriesAreChargedPerObservation pins the single largest accounting gap found
// in Stage 2: tx.obsKeys holds one heap string per observation, built as
// observerID+"|"+pathJSON (~109 bytes of payload on prod), and tx.observerSet
// holds one entry per distinct observer. Both live for the lifetime of the
// transmission. perTxMapsBytes charged a flat 200 bytes for the pair.
func TestObsDedupEntriesAreChargedPerObservation(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates; skipped under -short")
	}
	const nTx = 3000

	delta, held := measureHeapDelta(func() interface{} {
		maps := make([]*StoreTx, 0, nTx)
		for i := 0; i < nTx; i++ {
			tx := &StoreTx{
				obsKeys:     make(map[string]bool),
				observerSet: make(map[string]bool),
			}
			for j := 0; j < calObsPerTx; j++ {
				oid := freshString(calObserverIDLen, i*31+j)
				pj := freshString(calObsPathJSON, i*17+j)
				tx.obsKeys[oid+"|"+pj] = true
				tx.observerSet[oid] = true
			}
			maps = append(maps, tx)
		}
		return maps
	})

	runtime.KeepAlive(held)

	// Cost attributable to ONE observation's pair of dedup entries, excluding
	// the two map headers (charged once per tx via perTxMapsBytes).
	perObsActual := (float64(delta) - float64(nTx*perTxMapsBytes)) / float64(nTx*calObsPerTx)

	// The dedup component estimateStoreObsBytes now charges per observation.
	perObsCharged := float64(goSizeClass(calObserverIDLen+1+calObsPathJSON) + 2*mapEntryOverheadBytes)

	t.Logf("dedup entries: %.0f B/obs actual vs %.0f B/obs charged (%.0f B/tx measured total)",
		perObsActual, perObsCharged, float64(delta)/float64(nTx))

	if perObsActual <= 0 {
		t.Fatalf("fixture did not allocate as expected: %.0f B/obs", perObsActual)
	}
	if ratio := perObsCharged / perObsActual; ratio < 0.75 || ratio > 1.25 {
		t.Errorf("dedup-entry accounting is %.2fx actual (want 0.75-1.25): "+
			"charged %.0f B/obs, actual %.0f B/obs", ratio, perObsCharged, perObsActual)
	}
}

// TestStoreStructSizesMatchConstants fails when StoreTx or StoreObs gains or
// loses a field without storeTxBaseBytes / storeObsBaseBytes being updated.
// Production code stays free of unsafe; the check lives here instead.
func TestStoreStructSizesMatchConstants(t *testing.T) {
	if got := goSizeClass(int64(unsafe.Sizeof(StoreObs{}))); got != storeObsBaseBytes {
		t.Errorf("storeObsBaseBytes=%d but goSizeClass(sizeof(StoreObs))=%d (sizeof=%d) — "+
			"StoreObs changed shape; update the constant",
			storeObsBaseBytes, got, unsafe.Sizeof(StoreObs{}))
	}
	if got := goSizeClass(int64(unsafe.Sizeof(StoreTx{}))); got != storeTxBaseBytes {
		t.Errorf("storeTxBaseBytes=%d but goSizeClass(sizeof(StoreTx))=%d (sizeof=%d) — "+
			"StoreTx changed shape; update the constant",
			storeTxBaseBytes, got, unsafe.Sizeof(StoreTx{}))
	}
}

// TestTypicalEstimateAgreesWithRealEstimate keeps the cold-load budget
// estimator (estimateStoreTxBytesTypical, which prices a hypothetical record)
// aligned with the estimator the running store actually charges. They diverged
// before the 2026-08-08 store-accounting audit: the typical variant omitted the obsKeys/observerSet cost
// entirely, so the bounded cold load would happily load past its own budget.
func TestTypicalEstimateAgreesWithRealEstimate(t *testing.T) {
	const numObs = 25

	// A prod-shaped transmission priced by the REAL estimators.
	tx := &StoreTx{
		RawHex:      freshString(calTxRawHexLen, 1),
		Hash:        freshString(calTxHashLen, 2),
		DecodedJSON: freshString(calTxDecodedLen, 3),
		PathJSON:    freshString(calObsPathJSON, 4),
		FirstSeen:   freshString(calTxFirstSeen, 5),
	}
	hops := make([]string, calTxHops)
	for i := range hops {
		hops[i] = freshString(2, i)
	}
	tx.parsedPath = hops
	tx.pathParsed = true

	real := estimateStoreTxBytes(tx)
	for j := 0; j < numObs; j++ {
		snr, rssi := 1.0, -1.0
		real += estimateStoreObsBytes(&StoreObs{
			ObserverID:   freshString(calObserverIDLen, j),
			ObserverName: freshString(calObserverName, j),
			ObserverIATA: freshString(calObserverIATA, j),
			Direction:    freshString(calDirectionLen, j),
			PathJSON:     freshString(calObsPathJSON, j),
			Timestamp:    freshString(calTimestampLen, j),
			SNR:          &snr,
			RSSI:         &rssi,
		})
	}

	typical := estimateStoreTxBytesTypical(numObs)
	ratio := float64(typical) / float64(real)
	t.Logf("typical=%d real=%d ratio=%.3f (%d observations)", typical, real, ratio, numObs)

	if ratio < 0.9 || ratio > 1.1 {
		t.Errorf("estimateStoreTxBytesTypical is %.2fx estimateStoreTxBytes+Obs "+
			"(want 0.9-1.1): typical=%d real=%d", ratio, typical, real)
	}
}

// TestGoSizeClass covers the rounding ladder, including the short strings that
// dominate an observation and the large-object page path.
func TestGoSizeClass(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{0, 0}, {-5, 0},
		{1, 8}, {8, 8}, {9, 16},
		{2, 8},     // Direction
		{3, 8},     // ObserverIATA
		{14, 16},   // ObserverName
		{24, 24},   // Timestamp
		{44, 48},   // observation PathJSON
		{64, 64},   // ObserverID
		{109, 112}, // obsKeys dedup key
		{159, 160}, // tx RawHex
		{268, 288}, // tx DecodedJSON
		{8192, 8192},
		{8193, 16384},
		{20000, 24576},
	}
	for _, c := range cases {
		if got := goSizeClass(c.in); got != c.want {
			t.Errorf("goSizeClass(%d) = %d, want %d", c.in, got, c.want)
		}
	}
	// Never returns less than the request.
	for n := int64(1); n < 9000; n++ {
		if goSizeClass(n) < n {
			t.Fatalf("goSizeClass(%d) returned %d, smaller than the request", n, goSizeClass(n))
		}
	}
}

var _ = fmt.Sprintf
