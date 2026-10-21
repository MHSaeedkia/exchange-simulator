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

# Step 2 — nobitex (2026-10-21)

Facts in [[exchange-protocols]] § nobitex (flow + live probe). Decisions:

- **`Session.Subscribe(market, ack, snapshot bool)`** — the exchange decides whether a new
  subscriber gets a snapshot. nobitex: no (real nobitex sends none, AND a snapshot at the
  current offset would repeat the last push's offset → lpa-core `stale_or_duplicate`).
  okx: yes. The other Centrifugo full-book feeds (bitpin, ramzinex) will likely say no too.
- **Optional `exchange.Heartbeater`** — the server's write goroutine sends the exchange's own
  ping (`{}` every 25 s for nobitex) from connect on; not counted as a delivery. Pongs are not
  enforced (no close on a missing pong).
- Update = the WHOLE book (BookDepth levels), UPDATE_LEVELS = levels changed between pushes.
  `lastTradePrice` = best bid (the book has no trades).
- Not copied (NiFi never meets them; recorded in the package doc): close 3501 on a message
  before connect / bad JSON (Session has no Close), error 105 on a repeat subscribe (acked
  again instead), `offset` in the subscribe ack (the ack is built before the book is read).
- Centrifugo handshake code lives in the nobitex package for now; move it to a shared
  package when bitpin (the next Centrifugo exchange) needs it.
- `EXCHANGES_RUNNING` default stays `okx`: nobitex starts STOPPED unless listed.

Verified: `go test -race ./...` green. NOT run live (user: live checks after all exchanges).

# Step 3 — bitpin + shared Centrifugo package (2026-10-21)

- **`internal/exchange/centrifugo`**: the Centrifugo server protocol (connect / subscribe /
  unsubscribe / pong / error 102 / `{}` heartbeat / push envelope / SeqStep 1 / NiFi
  envelope). An exchange EMBEDS `*centrifugo.Protocol` and adds only its book frame.
  nobitex moved onto it; its protocol tests moved to the centrifugo package. ramzinex and
  ompfinex (also Centrifugo) should reuse it.
- `Protocol.Push(market, offset, data []byte)` keeps `data` byte for byte, because bitpin
  writes it with Python-style spaces (copied, ~10 % of the frame bytes — matters for load).
- `domain.Decimal.String()` added (prints all Scale decimals) for bitpin's `volume_*` sums.
- Not copied: bitpin's 2-decimal price padding ("84150.00" → here "84150").
- Verified: `go test -race ./...` green. NOT run live (live checks at the end).

# Step 4 — wallex (2026-10-21)

User: "i need 1, 2, 3 — this simulator should be exactly same as real" (ping/pong timeout,
30-min cut, 51-subscribe limit). Facts in [[exchange-protocols]] § wallex. Decisions:

- **Three optional contract parts** (okx/nobitex/bitpin untouched):
  `exchange.Connector` (`Connect(s) func(msg)`: speaks first + per-connection state in a
  closure, GC'd with the connection — the subscribe budget lives there);
  `exchange.KeepaliveRules` → `Keepalive{PingEvery, PingPayload, PongTimeout, MaxLife}`
  served by the server's write goroutine (control PING ticker, pong deadline timer reset from
  an atomic last-PONG, life timer; closes are 1000); `exchange.Channeler` (kafka mode turns a
  DB market into its wire streams).
- **Pong timeout rule = no PONG for 30 s since connect or the last PONG.** Fits both
  measurements (no PONG → closed at 30.0 s; with PONGs → lives to 30 min). An Engine.IO
  "ping + pingTimeout" reading (close at 50 s) does NOT fit the 30.0 s measurement.
- **A wallex channel is a feed market** (`BTCUSDT@buyDepth`, `BTCUSDT@sellDepth`): unsubscribing
  one side stops only that side, as on real wallex. So RECORDS counts channel frames and the
  report's markets count channels. Each frame = that side of the channel's book.
- Feed: a **nil ack sends nothing** (wallex never answers).
- `exchange.NewUUID` shared by centrifugo and wallex (kafka.NewID is the older copy, left alone).
- Inferred, not measured: a well-formed subscribe to an invalid channel uses a place.
- Verified: `go test -race ./...` green (server tests with ms-scale keepalive times). NOT run live.

# Step 4b — the small real behaviours of nobitex/bitpin (2026-10-21)

User: "first add them" (after wallex set the "exactly as real" rule). Probed real nobitex and
bitpin again for the exact rules (in [[exchange-protocols]] § Centrifugo rules). Two of my
step-2 guesses were WRONG and are now fixed: `{"id":3,"subscribe":{}}` closes 3501 (not error
102), and the prefix with no market is ACKED (no offset, no data), not error 102.

- `centrifugo.Protocol` is now a `Connector` (per-connection: connected?, subscribed channels).
- **`Session.Close(code, text)`** added (gorilla allows WriteControl beside the writer).
- **`Session.Subscribe(market, ack func(seq int64) []byte, snapshot)`**: the ack is built
  inside the feed goroutine from the book's current seq — atomic with the subscribe, so the
  Centrifugo ack's `offset` is exact. okx/wallex ignore seq (wallex passes nil).
- bitpin pads prices to `Shape.PriceScale()` (= decimals of BASE_PRICE/PRICE_STEP), new accessor.
- Verified: `go test -race ./...` green, server test for the 3501 close. NOT run live.
