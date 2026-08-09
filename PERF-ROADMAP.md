# Memory & Stability Roadmap

Status as of 2026-08-09. Supersedes the memory-related parts of
[PERFORMANCE.md](PERFORMANCE.md), which still documents the pre-Go
(`packet-store.js` / `server.js`) architecture and a 28K-packet dataset.

---

## The incident

Prod (`grav`, see [Deployment context](#deployment-context)) was restarting
**~104 times/day**, escalating from 8/day on 2026-08-03. Each restart cost 4-5
minutes of unusable API while the store reloaded.

### Root cause

The prod `config.json` had **no `packetStore` block at all**. Both
`retentionHours` and `maxMemoryMB` therefore defaulted to `0`, which means
*unlimited* — and `StartEvictionTicker()` returns a **no-op** when both are
zero:

```go
func (s *PacketStore) StartEvictionTicker() func() {
    if s.retentionHours <= 0 && s.maxMemoryMB <= 0 {
        return func() {} // no-op
    }
```

So nothing ever evicted. The store loaded the entire 2.2 GB database — 132,629
transmissions and 3,151,161 observations — and grew until it hit
`GOMEMLIMIT=5200MiB`, where the GC ran continuously on a 2-vCPU host and the API
stopped answering.

### The failure loop

1. Store loads all 2.2 GB → ~4.8 GB RSS, i.e. **88% of `GOMEMLIMIT`**
2. GC thrashes; `/api/stats` p95 hits 22 s, max 29 s; `/api/packets` p95 21.8 s
3. Watchdog's `curl --max-time 8` fails, and its curl-failure fallback was
   `age=999999` — indistinguishable from genuine ingest staleness
4. It therefore soft-recycled the **ingestor** (the wrong process; the *server*
   was sick), burning a 10-20 min cooldown
5. Still stale → hard container restart → 4-5 min cold reload → repeat

### Measurements

Sampled during a real cold load on 2026-08-07:

| observations | `trackedMB` (self-reported) | actual RSS |
|---|---|---|
| 1,422,025 | 601.9 | 2,337.8 |
| 2,069,300 | 881.2 | 3,352.8 |
| 3,150,500 (full) | 1,340.2 | **4,808.9** |

Retention windows measured against the real DB:

| window | transmissions | observations |
|---|---|---|
| 24h | 7,645 | 267,057 |
| 72h | 23,863 | 724,120 |
| 168h | 56,789 | 1,376,625 |

`last_seen` recency collapses repeat hashes, so these are far smaller than a
naive per-day extrapolation.

---

## Stage 0 — bound the store (DONE, 2026-08-07)

**Config only, no code.** Added to prod `config.json`:

```json
"packetStore": { "retentionHours": 72, "maxMemoryMB": 900 }
```

`retentionHours` is the binding constraint; `maxMemoryMB` is a backstop.

| | before | after |
|---|---|---|
| Process RSS | 4,809 MB | ~1,500-1,800 MB |
| …as % of `GOMEMLIMIT` | 88% | ~25% |
| Cold load | 4-5 min | 65 s |
| Avg API latency | 4,213 ms | 3.5 ms |
| `/api/packets` p95 | 21,803 ms | ~17 ms |
| Hard restarts | ~104/day | **0** |

Host swap also fell from 723 MB to 8 MB — the box was under real pressure that
was bleeding into every other service on it.

## Stage 1 — make the watchdog honest (DONE, 2026-08-07)

**Ops only, no code.** `/home/admin/corescope-watchdog.sh` rewritten to separate
two fault classes it previously conflated:

- **Fault A — ingest stale:** API answers, newest packet is old. Genuine dead
  MQTT subscription. Recycle the ingestor (its original purpose), escalate to a
  container restart if that doesn't clear it.
- **Fault B — API unreachable:** curl failed or timed out. The *server* is sick;
  recycling the ingestor cannot help. Restart the container directly, after 3
  consecutive failures.

Plus a 600 s startup grace (a cold load is legitimately slow; restarting mid-load
never converges) and a 20 s curl timeout instead of 8 s.

> Note: docker's `unless-stopped` policy does **not** act on health status, so
> the container healthcheck is informational. The watchdog is the only thing that
> restarts prod.

## Stage 2 — honest memory accounting (DONE in repo, NOT deployed)

Commit `788beb03`, branch `perf/store-accounting-activity-retention`.

`trackedBytes` under-reported real heap at **0.62×**. The gap was *not* a flat
per-observation ratio — it was concentrated in `perTxMapsBytes`, a flat 200 bytes
per **transmission** covering `tx.obsKeys` and `tx.observerSet`. Both maps gain
one entry per **observation** (24.64 obs/tx on prod, 109-byte keys), so the real
cost is ~6,300 B/tx — a ~31× undercount that a per-transmission constant cannot
model. A second gap: raw `len()` was counted instead of Go's allocator size
classes, and four string fields weren't counted at all.

Fixed via `goSizeClass()` plus per-observation dedup accounting. **Now 1.01×.**

Retention also now means **"active within N hours"**, not "first seen within N".
`LoadChunked` selects `last_seen >= cutoff` (per #1690) but eviction cut on
`FirstSeen`, discarding exactly the transmissions #1690 rescued — 151,785
observations (~26% of the cold load) dropped within 60 s of every boot.

**Deploy note:** this raises 72h tracked from ~233 MB to ~500 MB (retaining what
was previously discarded, plus honest accounting). Still under `maxMemoryMB=900`.
An honest 900 MB buys ~6.8 days, so raise the cap deliberately before going past
that.

## Stage 3 — shrink `StoreObs` (NOT STARTED)

Observations are ~90% of the heap (24:1 vs transmissions). Currently ~1.5 KB
each; target ~400-500 B, i.e. **~3× more history in the same RAM**.

1. `Timestamp string` → `int64` — also deletes the `sync.Once` + `time.Time` +
   `bool` parse-cache trio (~100 B/obs)
2. Intern `ObserverID` / `ObserverName` / `ObserverIATA` / `Direction` → a
   `uint16` index into the 30-row observer table + a `uint8` direction enum
3. `SNR`/`RSSI`/`Score` pointers → values + a presence bitmask (kills 3 heap
   allocations per observation)
4. `[]*StoreObs` → `[]StoreObs` — removes 3.15M individual heap objects. The
   largest GC-CPU win, since GC cost scales with object count.

**Interning is safe.** The v3 schema stores only `observations.observer_idx`;
`ObserverID`/`Name`/`IATA` are re-inflated per row by the load-time JOIN against
the 30-row `observers` table. They already resolve to *current* values, so there
is no per-observation history to lose. (v2 *did* store them per row — the
in-memory layout is a vestige of a schema the DB has since abandoned.)

Do it as four separate PRs so each is independently measurable and revertible.

## Stage 4 — two-tier store (NOT STARTED)

Hot RAM window for the live feed and analytics; serve the cold tail from SQLite
on demand. The indexes already exist (`idx_observations_tx_ts`,
`idx_observations_observer_idx_timestamp`), and packet-detail / trace views are
point lookups that don't need RAM residency.

## Stage 5 — fast startup (NOT STARTED)

`packetStore.hotStartupHours` exists and is disabled (`0`). Enabling it gives a
fast HTTP bind with background fill instead of a multi-minute cold load.

## Stage 6 — wire brush (NOT STARTED)

- Eviction filters several indexes in place (`slice[:0]` + `append`) across
  `byObserver`, `byPayloadType`, `byNode`, `distHops`, `distPaths`. The backing
  array tail past the new length still holds pointers to evicted objects, keeping
  them reachable. **`s.packets` was fixed in Stage 2; the others remain.**
- `/api/healthz` walks every packet counting observations under `RLock` — worse
  than `/api/stats` as a health probe. There is no cheap probe endpoint.
- `PERFORMANCE.md` documents an architecture that no longer exists.
- 267 `test-*.js` files in the repo root, including a 294 KB and a 184 KB one.
- `store.go` is 10.4K lines / 155 functions; `routes.go` is 3.7K / 75 handlers.
- Test runs leave `cmd/server/prune-requests/` litter; not gitignored.

---

## Open items

- **`retention.packetDays` is unset**, so DB packet pruning is disabled and the
  DB grows ~140 MB/day (2.2 GB over 16 days). Held pending sign-off because it is
  an irreversible delete. Disk was 69% used with ~68 days of runway.
- **Stage 2 is committed but not deployed.** CI builds the image on pushes to
  `master` / PRs targeting it; a branch push alone produces no image.

## Deployment context

Prod is **not** deployed from a compose file in this repo. On host `grav`:

| | |
|---|---|
| Deploy dir | `/home/admin/corescope-deploy/` (`docker-compose.yml`, `.env`) |
| Data dir → `/app/data` | `/home/admin/meshcore-data/` (`meshcore.db`, `config.json`) |
| Container | `corescope-deploy-corescope-1` |
| Bind | `127.0.0.1:8098` → `:3000`; nginx is the only path in |
| Limits | `mem_limit: 6g`, `GOMEMLIMIT=5200MiB` |
| Watchdog | `/home/admin/corescope-watchdog.sh` + `corescope-watchdog.{timer,service}` |

The compose file, prod `config.json`, and watchdog script are operator state that
exists only on that host — **repo changes alone never alter prod behaviour.**
