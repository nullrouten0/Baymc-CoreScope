package main

import (
	"testing"
	"time"
)

// Retention means "active within N hours", not "first seen within N hours"
// (decided 2026-08-08).
//
// LOAD (chunked_load.go) selects `t2.last_seen >= cutoff` — deliberately, per
// #1690: first_seen is written once and never updated, so filtering the cold
// load on it pulled only ~0.3% of the DB on prod and dropped long-lived hashes
// that still carry live traffic.
//
// EVICT used to cut on `tx.FirstSeen < cutoff`, which threw away exactly the
// transmissions #1690 set out to rescue: loaded on startup, dropped by the next
// eviction tick. On prod that was 151,785 observations (~26% of the cold load)
// discarded within 60s of boot, every boot. Eviction now judges by
// txActivityTime so the two halves agree.
func TestRetentionEvictsByActivityNotFirstSeen(t *testing.T) {
	s := NewPacketStore(nil, nil)
	s.retentionHours = 24

	now := time.Now().UTC()
	old := now.Add(-72 * time.Hour).Format(time.RFC3339)
	recent := now.Add(-1 * time.Hour).Format(time.RFC3339)

	// Long-lived hash: first seen 3 days ago, still being observed an hour ago.
	longLived := &StoreTx{ID: 1, Hash: "longlived", FirstSeen: old, LatestSeen: recent}
	// Genuinely stale: first seen and last active 3 days ago.
	stale := &StoreTx{ID: 2, Hash: "stale", FirstSeen: old, LatestSeen: old}
	// Recent: inside the window on both axes.
	fresh := &StoreTx{ID: 3, Hash: "fresh", FirstSeen: recent, LatestSeen: recent}

	for _, tx := range []*StoreTx{longLived, stale, fresh} {
		s.packets = append(s.packets, tx)
		s.byHash[tx.Hash] = tx
		s.byTxID[tx.ID] = tx
	}

	ids := s.evictionCandidateTxIDs()

	if len(ids) != 1 || ids[0] != stale.ID {
		t.Fatalf("expected only the inactive transmission (id=%d) to be evicted, got %v",
			stale.ID, ids)
	}
}

// TestRetentionEvictionIsNotPrefixBound guards the structural consequence of
// the activity axis: a retained transmission can sit BEFORE a stale one in
// s.packets (which is ordered by FirstSeen), so eviction can no longer assume
// its victims form a head prefix. A prefix-trimming implementation would either
// drop the live transmission or spare the stale one.
func TestRetentionEvictionIsNotPrefixBound(t *testing.T) {
	s := NewPacketStore(nil, nil)
	s.retentionHours = 24

	now := time.Now().UTC()
	oldest := now.Add(-96 * time.Hour).Format(time.RFC3339)
	older := now.Add(-72 * time.Hour).Format(time.RFC3339)
	recent := now.Add(-1 * time.Hour).Format(time.RFC3339)

	// Index 0 is the OLDEST by FirstSeen but still active -> must be retained.
	live := &StoreTx{ID: 1, Hash: "live", FirstSeen: oldest, LatestSeen: recent}
	// Index 1 is newer by FirstSeen but inactive -> must be evicted.
	dead := &StoreTx{ID: 2, Hash: "dead", FirstSeen: older, LatestSeen: older}

	for _, tx := range []*StoreTx{live, dead} {
		s.packets = append(s.packets, tx)
		s.byHash[tx.Hash] = tx
		s.byTxID[tx.ID] = tx
	}

	evicted := s.EvictStale()
	if evicted != 1 {
		t.Fatalf("expected exactly 1 eviction, got %d", evicted)
	}
	if len(s.packets) != 1 {
		t.Fatalf("expected 1 packet retained, got %d", len(s.packets))
	}
	if s.packets[0].ID != live.ID {
		t.Errorf("wrong survivor: kept id=%d, expected the still-active id=%d",
			s.packets[0].ID, live.ID)
	}
	if _, ok := s.byHash["dead"]; ok {
		t.Error("evicted transmission still present in byHash")
	}
	if _, ok := s.byHash["live"]; !ok {
		t.Error("retained transmission was removed from byHash")
	}
}

// TestEvictionNilsPacketsTail verifies the compaction does not leave evicted
// *StoreTx pointers live in the backing array past the new length. Those slots
// are still reachable through the slice's capacity, so a stale pointer there
// pins the transmission and every observation hanging off it until an equal
// number of new packets overwrite the slots.
func TestEvictionNilsPacketsTail(t *testing.T) {
	s := NewPacketStore(nil, nil)
	s.retentionHours = 24

	now := time.Now().UTC()
	old := now.Add(-72 * time.Hour).Format(time.RFC3339)
	recent := now.Add(-1 * time.Hour).Format(time.RFC3339)

	for i := 0; i < 10; i++ {
		ts, id := old, i+1
		if i >= 5 {
			ts = recent
		}
		tx := &StoreTx{
			ID:         id,
			Hash:       string(rune('a' + i)),
			FirstSeen:  ts,
			LatestSeen: ts,
		}
		s.packets = append(s.packets, tx)
		s.byHash[tx.Hash] = tx
		s.byTxID[tx.ID] = tx
	}

	if got := s.EvictStale(); got != 5 {
		t.Fatalf("expected 5 evictions, got %d", got)
	}
	if len(s.packets) != 5 {
		t.Fatalf("expected 5 retained, got %d", len(s.packets))
	}

	full := s.packets[:cap(s.packets)]
	for i := len(s.packets); i < len(full); i++ {
		if full[i] != nil {
			t.Errorf("backing array slot %d still holds *StoreTx id=%d — evicted "+
				"transmissions stay reachable and cannot be collected",
				i, full[i].ID)
		}
	}
}
