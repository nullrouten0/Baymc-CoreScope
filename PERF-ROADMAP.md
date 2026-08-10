# Memory & Stability Roadmap

Status 2026-08-10. Supersedes the memory-related parts of
[PERFORMANCE.md](PERFORMANCE.md), which still documents the pre-Go
(`packet-store.js` / `server.js`) architecture and a 28K-packet dataset.

---

## Resolved

Prod (`grav`) was restarting **~104×/day**. Root cause: the prod `config.json`
had **no `packetStore` block**, so `retentionHours` and `maxMemoryMB` both
defaulted to `0` = unlimited — and `StartEvictionTicker()` returns a **no-op**
when both are zero:

```go
if s.retentionHours <= 0 && s.maxMemoryMB <= 0 {
    return func() {} // no-op
}
```

Nothing ever evicted. The store loaded the whole 2.2 GB DB and grew until it hit
`GOMEMLIMIT`, where GC ran continuously on 2 vCPUs and the API stopped
answering. The host watchdog then misread its own curl timeout as ingest
staleness and recycled the wrong process.

| | before | after |
|---|---|---|
| Hard restarts | ~104/day | **0** |
| Process RSS | 4,809 MB | ~1,420 MB |
| …as % of `GOMEMLIMIT` | 88% | ~26% |
| Cold load | 4-5 min | 65 s |
| Avg API latency | 4,213 ms | ~3 ms |
| Host swap | 723 MB | 8 MB |

**Shipped:** `packetStore: {retentionHours: 72, maxMemoryMB: 900}`; a rewritten
watchdog that separates "API unreachable" (server sick → restart container) from
"ingest stale" (MQTT dead → recycle ingestor), with a 600 s startup grace; and a
2026-08-10 upgrade from `3c440c0` → `a06ac8ac` (63 commits) which alone cut RSS
by 500 MB via #1773.

---

## Active roadmap

Ordered by return per unit of risk. Everything here is measurable against
`/api/perf?mem=1`, which reports live per-component bytes.

### Tier 1 — operational, near-zero risk

**1. Put the watchdog in git.** `/home/admin/corescope-watchdog.sh` plus its
systemd units are the *only* thing that restarts prod, and they exist on one
host with no version control. Suggested home: `deploy/`. Zero risk, and it is
generically useful to anyone self-hosting.

**2. Cut a release.** Prod sat 8 weeks stale because it tracked `latest`, and
`latest` never moved — no release was cut after v3.9.2 while 63 commits landed.
The compose file is now digest-pinned so it cannot drift silently, but a release
is what makes `latest` trustworthy again.

**3. Deploy Stage 2** (already committed, branch
`perf/store-accounting-activity-retention`). Two fixes, both verified with
`-race`:
- `trackedBytes` was 0.62× real heap, so `maxMemoryMB` under-delivered by ~1.6×.
  Now 1.01×.
- Retention now means "active within N hours". Prod's own startup log shows the
  cost of the old behaviour: **813,545 observations loaded, 194,829 evicted
  within 30 s — 24% of cold-load work discarded.**

Deploy note: raises 72h tracked to ~500 MB (still under `maxMemoryMB=900`).

### Tier 2 — memory, low risk

Prod-measured baseline: **~743 B of live heap per observation**, and
`/api/perf?mem=1` reports `obsStringsMB: 110.25` against **11.5 MB for every
transmission string combined**. Observations are the entire problem.

| | Change | Saves | Why it's low risk |
|---|---|---|---|
| 4 | `tx.obsKeys` keys: the 109-byte `observerID\|pathJSON` string → a `uint64` hash | **~150 B/obs** | Internal to insert-time dedup; never read anywhere else |
| 5 | `Timestamp string` → `int64`, deleting the `sync.Once`+`time.Time`+`bool` parse-cache it feeds | **~88 B/obs** | `ParsedTime()` is already the accessor readers go through |
| 6 | Nil the backing-array tails in eviction's remaining in-place filters (`byObserver`, `byPayloadType`, `byNode`, `distHops`, `distPaths`) | leak fix | Small, contained; `s.packets` already done in Stage 2 |
| 7 | `SNR`/`RSSI`/`Score` pointers → inline values + presence bitmask | ~21 B/obs | Self-contained struct change; kills 3 allocs/obs |

### Tier 3 — biggest single win, moderate risk

**8. Intern `ObserverID`/`Name`/`IATA`/`Direction`** → a `uint16` index into the
30-row observer table plus a `uint8` direction enum. **~190 B/obs.**

Risk is breadth, not danger: it touches every reader of those fields, needs a
resolver at serialization time, and `byObserver` is currently keyed by the ID
string.

**Safety is settled, not assumed.** The v3 schema stores only
`observations.observer_idx`; the name/ID/IATA strings are re-inflated per row by
the load-time JOIN, so they already resolve to *current* values. There is no
per-observation history to lose. (v2 *did* store them per row — the in-memory
layout is a vestige of a schema the DB abandoned.)

**Tiers 2+3 together: ~743 → ~294 B/obs (~2.5×).** A week of retention would
drop from ~3.7 GB to ~1.5 GB.

---

## Dropped, and why

Removed 2026-08-10 as either invalidated by what we learned or bad
return-for-risk.

- **`[]*StoreObs` → `[]StoreObs`.** Worth only ~8 B/obs of bytes; the real prize
  was GC time. But `&slice[i]` points into the backing array and appends
  invalidate it, and `*StoreObs` is passed around freely across a 10K-line file.
  Severe risk for ~1% of the byte win.
- **Two-tier hot-RAM/cold-SQLite store.** *Premise was wrong.* SQLite already
  serves the cold tail: packet deep links fall back to it explicitly, and channel
  history, traces, observer metrics and node data are read from it directly. The
  store is a hot cache for list/analytics paths, not the sole read path.
- **`hotStartupHours` fast startup.** Bought a faster bind after a restart.
  Restarts went 104/day → 0, and cold load is 65 s. No longer worth doing.
- **Moving the 267 root `test-*.js` files, splitting `store.go`.** Churn across
  CI paths and a 10K-line file with no measurable return.
- **Cheap `/api/healthz`.** It does walk every packet under `RLock`, but nothing
  polls it. Fix it only if something starts to.

---

## Open item

**`retention.packetDays` is unset**, so DB packet pruning is disabled; the DB
grows ~140 MB/day (2.6 GB, 18 days). Deliberately left alone — it is an
irreversible delete, disk has ~65 days of runway, and pruning to 7 days would
degrade real features: channel history truncates, deep links to older packets
404, and `CountFloodAdvertsForNode` queries a 7-day window directly against
`transmissions`. If you ever bound it, **14 days is the floor**, and DB
retention must stay well above store retention.

---

## Deployment context

Prod is **not** deployed from a compose file in this repo. On host `grav`:

| | |
|---|---|
| Deploy dir | `/home/admin/corescope-deploy/` (`docker-compose.yml`, `.env`) |
| Data dir → `/app/data` | `/home/admin/meshcore-data/` (`meshcore.db`, `config.json`) |
| Container | `corescope-deploy-corescope-1`, image digest-pinned |
| Bind | `127.0.0.1:8098` → `:3000`; nginx is the only path in |
| Limits | `mem_limit: 6g`, `GOMEMLIMIT=5200MiB` |
| Watchdog | `/home/admin/corescope-watchdog.sh` + `corescope-watchdog.{timer,service}` |
| Rollback image | `corescope:rollback-v3.9.2-3c440c0` (pinned locally) |

The compose file, prod `config.json`, and watchdog script are operator state that
exists only on that host — **repo changes alone never alter prod behaviour.**
