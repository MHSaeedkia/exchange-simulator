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

# Step 5 — ramzinex (+ the Centrifugo pong rule for all three) (2026-10-21)

- Probing ramzinex found the **3012 "no pong"** close; re-probed nobitex/bitpin: they do it
  too (8 s). Added for all three: `exchange.Heartbeat` is now a struct (Msg, Every,
  PongTimeout, IsPong, CloseCode/Text); the server marks a pong when IsPong(msg) on read and
  checks it PongTimeout after each text ping.
- `centrifugo.New(Options{Prefix, Version ("" = none), Epoch ("" = 8 letters), AnyNamespace,
  PingEvery, PongTimeout})` — where the real servers differ.
- **Server prefix routes**: an endpoint path ending in "/" serves every path under it
  (longest prefix wins, exact first) — ramzinex's REST path has the market inside it.
- ramzinex level metadata: el6 = 8 + fnv(price:qty)%83, el7 = now − fnv%25 min.
- Verified: `go test -race ./...` green (server tests for 3012 and prefix routes). NOT live.

# Step 6 — bitget (2026-10-21)

- **Optional `exchange.IdleRules` → `Idle{Limit, Check, IsAlive}`**: the server marks a client
  alive when IsAlive(msg) on read and sweeps every Check from a random first phase; a quiet
  client is dropped with `c.close()` (TCP cut, no close frame) — real bitget's 2-min rule.
- `subscribe` passes snapshot=false: every books50 frame is a whole snapshot, and a snapshot
  to one new subscriber at the current seq would repeat a seq other subscribers already saw.
- Replies built with a non-HTML-escaping JSON encoder (bitget's error msg echoes raw text).
- Verified: `go test -race ./...` green (server tests for the idle cut and ping keeping it open). NOT live.

# Step 7 — bybit (2026-10-21)

- `Connector` for the per-connection conn_id and subscribed topics (a repeat is silent).
- One ack per message, sent before the first new topic's snapshot (ack func on the first
  Subscribe only); snapshot=true (real bybit sends it right after the ack).
- `seq` derived from u: seqBase + 17u + fnv(u)%13 (rises 5…29 per u, as real 4…30).
  REST `u` = WS u / 5 (a different, smaller counter, as real; nobody reads it).
- Probe finished after the commit: real bybit did NOT cut a no-ping client in 11 minutes →
  no Idle rule for bybit.
- Verified: `go test -race ./...` green. NOT live.

# Step 8 — ompfinex (+ the unasked-pong fix for all Centrifugo) (2026-10-21)

- **Centrifugo pong fix:** an unasked-for `{}` closes 3501 on all four real servers (my
  earlier code took any `{}` as a pong). Now the SERVER consumes a heartbeat answer only while
  a ping is outstanding (`conn.lastPing` > `lastPong`) — it never reaches the exchange; any
  other `{}` reaches the Centrifugo handler, which closes 3501.
- `centrifugo.Options.PlainAck` for ompfinex's `{"id":N,"subscribe":{}}`.
- **REST-only snapshot:** optional `WebSocketSnapshots() bool` on the Encoder (feed's
  `wsSnapshotter`): false → in websocket mode snapshotTo-everyone does nothing (no frame AND
  no seq step, so the delta chain stays unbroken). In kafka mode the snapshot is sent: it is
  the REST body + `"action":"snapshot","pair":…` at the end, as NiFi's Jolt publishes it.
- ompfinex keeps a per-market Centrifugo offset (+1 per push) apart from u (step 1…3);
  safe without a lock because Snapshot/Update run on the feed goroutine only.
- Verified: `go test -race ./...` green. NOT live.

# Step 9 — lbank (2026-10-21) — all 9 exchanges built

- `Heartbeat` grew: `MsgFor(connID, now)` (lbank's ping carries its send time), `First`
  (first ping delay; lbank: random in the first minute), `IsPong(connID, msg)` (lbank checks the
  id against the pings that connection got), `MissedLimit` (lbank: 6 unanswered → close 1001
  when the 7th is due). The server counts missed pings at each tick.
- lbank's handler sees only repeat/stranger pongs (the server takes the outstanding one).
- Startup check: the binary loads all 9 (`/status` lists 9), ports 443 + 8443 + proxy 2053.
- **Gotcha found:** a simulator started for the step-1b proxy check kept running for ~1.5 h:
  the "stopped" check used `pgrep -f scratchpad/run/sim` but the process command line was
  `./sim -env …`. Stop background test binaries by PID and check the PID.
- Verified: `go test -race ./...` green. NOT live (live checks at the end).

# Real-scenario test (2026-10-21, user: "docker is up … test all thing in real scenario")

User chose lpa-core's `docker-compose.prod.yml`. Started only kafka, schema-registry, postgres,
redis, nifi, jobmanager, taskmanager, with env overrides (no lpa-core file edited):
`NIFI_JVM_HEAP_INIT/MAX=1536m TASKMANAGER_MEMORY=2500m`. warmup → topics existed (4811);
the 10 Flink jobs submitted with `flink/run-job.sh` in Makefile order (all RUNNING).

**Kafka mode — VERIFIED for all 9:** simulator container in kafka mode, 50 frames/s each,
~1 min. For every exchange `ex{id}-raw` = `ex{id}-parsed-flink` = pair-raw = type-validated,
**0 rejects** (nobitex 3699, bitpin 3696, wallex 3755, ramzinex 3695, bitget 3674, bybit 3675,
ompfinex 3699, lbank 3676; okx run 2: 3027). (`MARKET_STATUS=unsubscribe` to use all seeded rows
of 8 exchanges without touching the DB; okx separately with `subscribe`.)

**WebSocket mode through real NiFi — what was VERIFIED:**
- Image builds (test stage runs). `docker exec nifi getent hosts <all 14 hosts>` → simulator.
- CA imported into NiFi's JVM cacerts + restart → NiFi's TLS to every exchange host verifies.
- Flow imported into a fresh NiFi with `POST /nifi-api/process-groups/{root}/process-groups/upload`
  (Data-Collection.json); CONFIG: `postgres.password=postgres` (compose default),
  `proxy_server.host=exchange-simulator` (port unchanged) via a parameter-context update request;
  all 31 controller services ENABLED + VALID; 684 processors running (430 = the disabled Private copy).
- **All 9 exchanges connected through NiFi** (wallex 10 sockets); okx and lbank WS **through the
  simulator's proxy**. 18 markets (BTC+ETH ×9) set to `pending-subscribe` in the DB → NiFi's poller
  sent them, simulator took them, PutSQL set all 18 to `subscribe`; all 9 streamed; 0 missed.
- REST through the redirect: ompfinex (DNS) and bybit + okx (**through the proxy**) answered
  lpa-core's snapshot requests.

**Found during the NiFi run:**
- Local DB lacked `exchange_sockets` (made before 2026-10-03) → every wallex `connected` step
  failed and retried forever. Added the table + seed exactly from lpa-core's 01_schema/02_seed.
- ompfinex's first connection was closed 3012 by the simulator: NiFi's first pong came too late
  (likely before NiFi's connected path stored the session in Redis); real ompfinex has the same
  4 s rule. The reconnected socket stayed up.
- Rejects `stale_or_duplicate` on sequenced exchanges: an ARTIFACT of running the simulator twice
  against the same Flink state (books restart at SEQ_START, Flink remembers the old max).
  ⇒ **a simulator restart while Flink keeps state causes a reject burst** — open: decide a fix.
- `ex{id}-raw` end offsets ≈ 2× frames through NiFi: PublishKafka uses transactions; commit
  markers / aborted records take offsets. Count records, not offsets, on NiFi-fed topics.
- **Docker VM (8 GB of the Mac's 16) is too small:** the VM kernel OOM-killed the taskmanager once
  and Kafka twice (`dmesg` in a privileged container shows `global_oom`; Docker's OOMKilled flag
  stays false). Kafka's prod heap is a fixed 2 GB. Each Kafka restart recovers 4912 logs.
- NiFi left as configured for the test (flow imported, CA in cacerts, proxy param pointing at
  the simulator, 18 rows `subscribe`, `exchange_sockets` added).

# Restart fix: sequence numbers start from the clock (2026-10-21, user chose it)

`domain.ClockSeqStart(base, stepMax, now) = base + ms since 2026-01-01 × stepMax`; main passes
it as each feed's SeqStart (both modes). A later run starts above anything an earlier run could
reach unless one market ran > 1000 frames/s. Stays < 2^53 (exact for JSON-double readers) for
~20 years even with bitget's 12000 step. SEQ_START is now the BASE. Not covered: ompfinex's
Centrifugo offset counter restarts at its constant (the parser orders on u, not offset).

# Real-scenario test, part 2 (2026-10-21, Docker raised to 12 GB by the user)

- Docker VM restart → jobmanager (no HA) lost the jobs → resubmitted; NiFi kept its CA (same
  heap env, so compose did not recreate it).
- **REAL-EXCHANGE LEAK found:** while the simulator container was down, the aliases vanished,
  NiFi reconnected 6 exchanges to the REAL exchanges (seen in /proc/net/tcp: wallex's real IP
  ×10 etc.) and stayed there after the simulator came back. Fixed by Stop/Start of all 18
  ConnectWebSockets; documented in docs/REDIRECT.md (keep the simulator up; check NiFi's TCP
  connections after any simulator restart).
- Stopping a ConnectWebSocket runs no "disconnected" path → rows stay `subscribe`, never resent;
  set them to `pending-subscribe` (documented).
- **Clean 2-min window through real NiFi, all 9: 0 rejects, 0 missed, 0 send errors, no OOM;**
  every parsed record passed the type validator; the parser rate matches the simulator's 20/s.
- ompfinex 3012 closes only while Redis was down at NiFi start (its ping path needs Redis):
  real ompfinex would cut the same.
- **Correction:** NiFi DOES ping bitget (25 s) and bybit (15 s) through timer-driven paths; my
  trace tool missed them. No idle drops are expected or seen. Lesson: when reading a NiFi
  group, also follow paths rooted at GenerateFlowFile (any processor with no incoming
  connection), not only ConnectWebSocket and the input port.

# Defaults from the user's .env (2026-10-22)

User: "move my .env changes into .env.example and commit". Chose to change the code defaults
too, so `.env.example` stays equal to the built-in defaults (its header promises that):
`EXCHANGES_RUNNING` = all 9; okx book `84000 / 0.01 / 0.000001…0.200000`. The config test's
"too deep" OKX_BOOK_DEPTH moved 1e6 → 1e8 (with step 0.01 a 1e6-level book is now valid).
