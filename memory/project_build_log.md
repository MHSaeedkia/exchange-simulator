---
name: build-log
description: Dated record of each build step of exchange-simulator — what was built, the decisions taken while building, and what was and was not verified
metadata:
  type: project
---

# Step 1 — project + shared core + okx (2026-10-21)

Copied from lpa-core `load-generator/` at `537a0af` (no git history carried over: the branch
had one commit) and reshaped for many exchanges. See [[origin]] for the inherited design.

## Decisions taken while building

- **`exchange.Exchange` is the one contract per exchange**: frames (Snapshot, Update),
  sequence step, WS message handling (OnMessage gets a Session that can Send / Subscribe /
  Unsubscribe), REST (ServeREST reads a Books interface), Kafka envelope. Each exchange is a
  sub-package; `main.newExchange` has one case per exchange. Mock orders will be a SECOND,
  optional interface — the market-data contract stays as is.
- **The book is generic** (`domain.Book`: levels + a sequence number; `Requote` changes
  levels, `Advance` moves the number). How the number moves and how a frame looks is the
  exchange's job (okx: random step, checksum, prevSeqId).
- **One `feed.Feed` per exchange** — own goroutine, own rate, own books — so one exchange's
  schedule never waits for another's. Only a shared slow socket or a slow Kafka can slow
  them all, and that is a real test result.
- **Stopped = silent** is implemented in the feed: subscribes are still acked and recorded,
  no snapshot/update is sent; Start sends every active market a fresh snapshot first.
- **A snapshot sent to EVERYONE (start, new depth, a market back in kafka mode's DB list)
  advances the sequence one step first.** Found by a test: after stop/start the snapshot had
  the same seqId as the last update, and lpa-core's type validator drops a sequenced snapshot
  that is not above the last one (`stale_or_duplicate`) — every start would have added a
  reject to the test. A snapshot to ONE new subscriber keeps the current seqId, as real okx.
- **Report rows are per stage with per-exchange columns** (`okx.sent_per_sec`, …) + totals.
  **A stage ends at the counters taken JUST BEFORE the change** (`Recorder.Switch(now,
  before, after)`): found live — a start's snapshots were counted in the stopped stage.
- **Control API**: `POST /exchanges/{name}/start|stop|settings`; a start of a running
  exchange (or stop of a stopped one) changes nothing and starts no stage.
- **Config**: global keys plain, each exchange a block under its upper-case prefix; the
  defaults are the REAL addresses (that is what makes the redirect need no NiFi change).
  `EXCHANGES_RUNNING=none` starts with all stopped. Kafka topic is a pattern `ex{id}-raw`.
- Redirect decisions are in [[redirect]].

## Verified

- `go vet`, gofmt, `go test -race ./...` — 11 packages green.
- Live, local binary with TLS on the real okx ports (REST moved to 9443, macOS gotcha in
  [[redirect]]): a NiFi-like client trusting only our CA dialled `wss://ws.okx.com:8443`:
  1000/s held (1000.67, 1000.00, 999.01 per stage), 0 data while stopped, fresh snapshot on
  start, 50-level snapshots after a depth change, chain unbroken in every phase; REST over
  TLS with `curl --resolve`; a client without the CA refused; report rows per stage correct.

## NOT verified

- kafka mode in this layout (Docker was down) — the okx-only version ran it with 0 rejects.
- Anything with NiFi; the docker network alias; the Docker image build.

# Step 1b — HTTP proxy for NiFi's proxied calls (2026-10-21)

The NiFi flow showed okx WS + REST go through an HTTP proxy that DNS cannot catch. Built
`internal/server/proxy.go` + `PROXY_PORT` (0 = off). Why and how: [[redirect]] § The proxy.
`docs/REDIRECT.md` steps 3 (JVM `cacerts`, from the flow) and 5 (change `proxy_server.host`)
rewritten. Verified: `go test -race ./...` green, live check as in [[redirect]]. NOT verified:
anything with NiFi.
