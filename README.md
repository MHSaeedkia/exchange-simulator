# exchange-simulator

Plays the crypto exchanges that lpa-core ingests, for load tests. Each exchange sends
its own real frames at a rate you choose. The tool writes how the system behaved at
each rate.

- **websocket mode** — fake exchange servers on the **real** host names, ports and
  paths (for example `wss://ws.okx.com:8443/ws/v5/public`). With the host names
  pointed at the simulator, NiFi connects to it with **no flow change**. See
  [docs/REDIRECT.md](docs/REDIRECT.md).
- **kafka mode** — writes straight to each exchange's `ex{id}-raw` topic, without NiFi.
  The markets come from the database.

Every setting is in `.env` (see `.env.example`). Nothing is hard coded.

**Exchanges:** all 9 — nobitex (1), bitpin (2), wallex (3), ramzinex (4), bitget (5), bybit (6),
ompfinex (7), okx (8), lbank (9).

## Run it

```sh
cp .env.example .env     # first time only, then edit .env
go run .                 # or: go build -mod=vendor -o exchange-simulator . && ./exchange-simulator
```

Flags: `-env <file>` (default `.env`), `-log-level debug|info|warn|error`,
`-gen-certs` (make the certificates and exit).

Stop with Ctrl+C or `POST /stop`. Both write the report first.

In Docker, beside NiFi: `docker compose -f deploy/docker-compose.yml --env-file .env up -d --build`.

## Control API (`API_ADDR`)

```sh
curl localhost:9501/status                                   # every exchange: running, settings, markets, counters
curl -XPOST localhost:9501/exchanges/okx/start               # send data
curl -XPOST localhost:9501/exchanges/okx/stop                # silent: keeps connections, sends no data
curl -XPOST localhost:9501/exchanges/okx/settings -d '{"records":500,"period_ms":100}'
curl -XPOST localhost:9501/exchanges/okx/settings -d '{"book_depth":50,"update_levels":10}'
curl -XPOST localhost:9501/stop                              # end the last stage, write the report, exit
```

- Each exchange has its own rate: `records` updates every `period_ms` milliseconds, in
  total for all its markets.
- Every start, stop or settings change starts a new **stage** in the report.
- A bad value (for example a book so deep that the lowest bid is ≤ 0) gets a 400 and
  changes nothing.
- `EXCHANGES_RUNNING` in `.env` says which exchanges send data from the start.

## What every fake exchange does

- **Before a subscribe:** nothing (except the exchange's own pings). **On subscribe:** the
  exchange's ack, then a snapshot (for exchanges that send one), then updates. **On unsubscribe:** an ack, and that market stops.
- **Sequence numbers never break.** Each update names the one before it, the way that
  exchange does. A snapshot sent to everyone (after a start or a new book depth) moves the
  sequence forward first, so the pipeline takes it as newer.
- **Sending never drops a frame.** When a client or Kafka is slow, the tool waits. If it
  falls more than one period behind, the late updates are counted as **missed** and the
  schedule starts again. A client that cannot take a frame within `WS_WRITE_TIMEOUT_MS`
  is disconnected.
- **Stopped = silent.** Connections and subscriptions stay, acks and pings are answered,
  but no market data is sent. A start sends a fresh snapshot first.

### okx (8)

| Client sends | Fake answers |
|---|---|
| `{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}` | ack `{"event":"subscribe",…,"connId":…}`, then a snapshot (`prevSeqId: -1`), then updates |
| `{"op":"unsubscribe",…}` | ack `{"event":"unsubscribe",…}`; that market stops |
| `ping` | `pong` |
| bad JSON / unknown `op` / no `args` | `{"event":"error","code":"60012",…}` |
| channel is not `books`, or no `instId` | `{"event":"error","code":"60018",…}` |
| `GET /api/v5/market/books?instId=…&sz=…` | `{"code":"0","msg":"","data":[{asks,bids,ts,seqId}]}` |

`seqId` moves by a random step (`OKX_SEQ_STEP_MIN`…`OKX_SEQ_STEP_MAX`); `checksum` follows
okx's rule (CRC32 of the best 25 levels).

### nobitex (1)

Centrifugo at `wss://ws.nobitex.ir/connection/websocket`. No REST.

| Client sends | Fake answers |
|---|---|
| `{"id":1,"connect":{}}` | `{"id":1,"connect":{"client":…,"version":"6.9.3 OSS","ping":25,"pong":true}}` |
| `{"id":1,"subscribe":{"channel":"public:orderbook-BTCUSDT"}}` | `{"id":1,"subscribe":{"recoverable":true,"epoch":…,"offset":<current>,"positioned":true}}`, then pushes (no snapshot) |
| the same subscribe again on one connection | `{"id":1,"error":{"code":105,"message":"already subscribed"}}` |
| `{"id":1,"unsubscribe":{"channel":"public:orderbook-BTCUSDT"}}` | `{"id":1,"unsubscribe":{}}`; that market stops |
| `{}` (answer to a ping) | nothing |
| a channel of another namespace | `{"id":1,"error":{"code":102,"message":"unknown channel"}}` |
| `public:orderbook-` (no market) | an ack with no offset, no data |
| anything before connect; bad JSON; a second connect; an unknown command; a command with no `id`; a subscribe with no channel | close `3501 bad request` |
| (server, every 25 s) | `{}`; no `{}` back within 8 s → close `3012 no pong` |
| `{}` when no ping is waiting | close `3501 bad request` |

Every push is the WHOLE book: `{"push":{"channel":…,"pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}`.
`offset` moves by one per push.

### bitpin (2)

Centrifugo at `wss://centrifugo.bitpin.ir/connection/websocket`, channel `orderbook:BTC_USDT`.
No REST. The same protocol and replies as nobitex (version `6.9.2 OSS`). Every push is the
WHOLE book (50 levels), written as bitpin writes it, with spaces and prices padded to the
decimals of `BITPIN_PRICE_STEP` (`"84150.00"`):
`{"push":{"channel":…,"pub":{"data":{"bids": […], "asks": […], "volume_ask": …, "volume_bid": …, "symbol": …, "event": "market_data", "price": …, "event_time": "…Z"},"offset":N}}}`.

### ramzinex (4)

Centrifugo at `wss://websocket.ramzinex.com/websocket`, channel `orderbook:12` (numeric market
ids). The nobitex protocol with ramzinex's differences: no `version` in the connect reply, a
number-like epoch, any channel namespace acked (no data), 10 s to answer a ping. Every push is
the WHOLE book, both sides price-descending:
`{"push":{"channel":"orderbook:12","pub":{"data":{"buys":[[84048,0.000042,3.530016,false,null,10,1792611279396],…],"sells":[…]},"offset":N}}}`.
REST: `GET https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/12/buys_sells` →
`{"data":{"buys":[…],"sells":[…]},"status":0}`.

### bitget (5)

`wss://ws.bitget.com/v2/ws/public`, channel `books50`. No REST.

| Client sends | Fake answers |
|---|---|
| `{"op":"subscribe","args":[{"channel":"books50","instType":"SPOT","instId":"BTCUSDT"}]}` | `{"event":"subscribe","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"}}` per arg, then frames (no extra snapshot) |
| `{"op":"unsubscribe",…}` | `{"event":"unsubscribe","arg":{…}}`; that market stops |
| `ping` | `pong` |
| text it cannot read | `{"event":"error","code":30002,"msg":"Unrecognized request:…"}` |
| another channel | `{"event":"error","arg":{…},"code":30016,"msg":"Param error","op":"subscribe"}` |
| no `ping` for 2 minutes | dropped, no close frame |

Every frame is the whole book: `{"action":"snapshot","arg":{…},"data":[{"asks":[…],"bids":[…],"ts":"…","seq":N,"pseq":0}],"ts":…}`;
`seq` moves by thousands (`BITGET_SEQ_STEP_MIN`…`MAX`).

### bybit (6)

`wss://stream.bybit.com/v5/public/spot`, topic `orderbook.50.BTCUSDT`; REST
`https://api.bybit.com/v5/market/orderbook` (NiFi calls it through its proxy).

| Client sends | Fake answers |
|---|---|
| `{"op":"subscribe","args":["orderbook.50.BTCUSDT"]}` | `{"success":true,"ret_msg":"subscribe","conn_id":…,"op":"subscribe"}`, then a snapshot, then deltas |
| the same topic again on one connection | nothing |
| more than 10 args | `{"success":false,"ret_msg":"args size \u003e10",…}`, nothing subscribed |
| `{"op":"unsubscribe",…}` | `{"success":true,"ret_msg":"unsubscribe",…}`; that market stops |
| `{"op":"ping"}` | `{"success":true,"ret_msg":"pong",…,"op":"ping"}` |
| not JSON (even `ping`) | `{"type":"error","ret_code":"400","ret_msg":"Invalid JSON!"}` |
| `GET …/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=50` | `{"retCode":0,"retMsg":"OK","result":{s,a,b,ts,u,seq,cts},"retExtInfo":{},"time":…}` |

Frames: `{"topic":…,"ts":…,"type":"snapshot"|"delta","data":{"s":…,"b":[…],"a":[…],"u":N,"seq":M},"cts":…}`;
a delta has only the changed levels, an unchanged side is `[]`.

### ompfinex (7)

Centrifugo at `wss://stream.ompfinex.com/stream`, channel `public-market:r-depth-14`. The nobitex
protocol with ompfinex's differences: version `6.9.0 OSS`, a ping every 6 s with 4 s to answer,
and a plain ack `{"id":2,"subscribe":{}}`. The WebSocket carries **deltas only**:
`{"push":{"channel":…,"pub":{"data":{"u":N,"U":<previous u>,"a":[…],"b":[…]},"offset":M}}}`.
A book's start is the REST snapshot `GET https://api.ompfinex.com/v1/market/14/depth?limit=50` →
`{"status":"OK","data":{"lastUpdateId":N,"time":<µs>,"bids":[…],"asks":[…]}}`, which lpa-core asks
for when a delta finds no baseline.

### lbank (9)

`wss://www.lbank.com/old-wss/ccws/ws/V3`; REST `https://api.lbkex.com/v2/depth.do` (NiFi reaches
both through its proxy).

| Client sends | Fake answers |
|---|---|
| `{"symbol": "btc_usdt","pair": "btc_usdt","action": "subscribe","subscribe": "fdepth","depth": "50",…}` | plain text `<token>_fdepth_btc_usdt_50`, then the whole book at once |
| the same with `"action": "unsubscribe"` | nothing; that market stops |
| (server, every 60 s; the first at a random point of the first minute) | `{"ping":"<ms>","action":"ping"}`; 6 unanswered in a row → close `1001 server-heartbeat-timeout` |
| `{"pong":"<that ms>","action":"pong"}` | nothing; any other id → `{"SERVER":"V3","message":"[id] is not a ping",…}` |
| text it cannot read (NiFi echoes the 59-byte ack: it takes ≤ 60-byte frames as pings) | `{"SERVER":"V3","message":"message","status":"error","TS":…}` |
| no `depth` | `{"SERVER":"V3","message":"Missing parameter ['depth']",…}` |

Frames: `{"depth":{"asks":[…],"bids":[…]},"SERVER":"V3","count":50,"limit":50,"type":"fdepth","pair":"btc_usdt","TS":"2026-10-22T04:16:24.032"}`
— `TS` is **China time (UTC+8)**, as real lbank sends it.

### wallex (3)

`wss://api.wallex.ir/ws`. No REST. Copies real wallex exactly:

| When | Fake does |
|---|---|
| connect | sends `{"sid":…,"upgrades":[],"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}` |
| `["subscribe", {"channel": "BTCUSDT@buyDepth"}]` | no reply, no snapshot; that channel's frames start |
| `["unsubscribe", {"channel": "BTCUSDT@buyDepth"}]` | no reply; that channel stops |
| subscribe #52 on one connection | ignored, no reply (repeats count, unsubscribes do not) |
| bad message | ignored |
| every 20 s | control PING `ping` |
| no PONG for 30 s | close 1000 |
| 30 minutes after connect | close 1000, always |

A frame is one channel's whole side: `["BTCUSDT@sellDepth",[{"price":84356.47,"quantity":0.00035696,"sum":30.1118855312},…]]`
(numbers, `sum` = price × quantity). In kafka mode a DB market sends both channels.

## kafka mode

Set `OUTPUT_MODE=kafka`. Each exchange sends every `exchange_markets` row of its id whose
status is `MARKET_STATUS`, re-read every `DB_REFRESH_MS`. Each frame gets what NiFi would
add (`id`, `simulation`). The tool never creates topics; a missing one stops it at start.

⚠ In kafka mode nobody answers the pipeline's snapshot requests (NiFi does that in
websocket mode). This matters only if Flink loses its state during a run; then change the
exchange's `book_depth` (a fresh snapshot) or restart the tool. Start the tool after the
Flink jobs are running.

## Report

Each run writes to `REPORT_DIR/run-<date>-<time>/`:

- `stages.csv` / `stages.json`: one row per stage. Totals (`target_per_sec`,
  `sent_per_sec`), then for each exchange `<name>.running`, `.records`, `.period_ms`,
  `.book_depth`, `.update_levels`, `.markets`, `.active_seconds`, `.target_per_sec`,
  `.sent_per_sec`, `.updates`, `.snapshots`, `.deliveries`, `.missed`, `.send_errors`,
  `.rest_calls`, then `_avg` / `_max` of every sampled system number.
- `samples.csv`: every sample, as `time, stage, metric, value`.

`sent_per_sec` is updates divided by `active_seconds`: the time the exchange was running
and had at least one market. The time before NiFi subscribes does not count.

Sampled every `SAMPLE_INTERVAL_MS`:

- **Docker** (`MONITOR_CONTAINERS`, `none` = off): `docker.<name>.cpu_pct` (100 = one
  core), `.mem_mib`, `.net_rx_kib_s`, `.net_tx_kib_s`, from the Docker Engine API at
  `DOCKER_HOST`.
- **Prometheus** (`PROMETHEUS_URL`, empty = off): `prom.<name>` for each line
  `name: promql` in `PROM_QUERIES_FILE`. Each query must return one number. See
  `queries.example.txt`.

A source that fails is logged once and skipped. It does not stop the test.

## Layout

| Path | What it does |
|---|---|
| `main.go` | wiring: config, one feed per exchange, fake servers or Kafka, API, sampling, report |
| `internal/config` | `.env` → `Config`: global settings + one block per exchange |
| `internal/domain` | the book, settings, counters, endpoints |
| `internal/exchange` | the contract every exchange implements |
| `internal/exchange/ompfinex` | ompfinex: delta frames and the REST depth (its only snapshot) |
| `internal/exchange/okx` | okx: frames, protocol replies, REST, what NiFi adds |
| `internal/exchange/bybit` | bybit: orderbook.50 frames, replies, REST order book |
| `internal/exchange/centrifugo` | the Centrifugo protocol shared by nobitex and bitpin: connect, subscribe, ping, push envelope |
| `internal/exchange/lbank` | lbank: fdepth frames, plain-text acks, its ping/pong, REST depth |
| `internal/exchange/nobitex` | nobitex: its book frame |
| `internal/exchange/bitget` | bitget: books50 frames, replies, the idle cut |
| `internal/exchange/bitpin` | bitpin: its book frame |
| `internal/exchange/ramzinex` | ramzinex: its book frame and REST order book |
| `internal/exchange/wallex` | wallex: side frames, the open message, the 51-subscribe budget, keepalive rules |
| `internal/feed` | one generator per exchange: owns its books, keeps the rate, sends every frame in order |
| `internal/server` | one listener per port, TLS, routing by host name + path, WebSocket connections |
| `internal/certs` | the test CA and the server certificate |
| `internal/kafka` | kafka mode: producer + topic check |
| `internal/postgres` | kafka mode: the market list |
| `internal/monitor` | Docker + Prometheus sampling |
| `internal/report` | stages and samples → files |
| `internal/httpapi` | the control API |
| `deploy/` | compose file: the simulator in lpa-core's network, with the exchange host names as aliases |
| `docs/` | `REDIRECT.md` (the procedure), `sample-raw-data.md` (the wire reference, from lpa-core) |

## Test

```sh
go test -mod=vendor ./...
```
