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

**Exchanges so far:** okx (8), nobitex (1), bitpin (2). The others are added one by one:
wallex (3), ramzinex (4), bitget (5), bybit (6), ompfinex (7), lbank (9).

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
| `{"id":1,"subscribe":{"channel":"public:orderbook-BTCUSDT"}}` | `{"id":1,"subscribe":{"recoverable":true,"epoch":…,"positioned":true}}`, then pushes (no snapshot) |
| `{"id":1,"unsubscribe":{"channel":"public:orderbook-BTCUSDT"}}` | `{"id":1,"unsubscribe":{}}`; that market stops |
| `{}` (answer to a ping) | nothing |
| a channel outside `public:orderbook-` | `{"id":1,"error":{"code":102,"message":"unknown channel"}}` |
| (server, every 25 s) | `{}` |

Every push is the WHOLE book: `{"push":{"channel":…,"pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}`.
`offset` moves by one per push.

### bitpin (2)

Centrifugo at `wss://centrifugo.bitpin.ir/connection/websocket`, channel `orderbook:BTC_USDT`.
No REST. The same protocol and replies as nobitex (version `6.9.2 OSS`). Every push is the
WHOLE book (50 levels), written as bitpin writes it, with spaces:
`{"push":{"channel":…,"pub":{"data":{"bids": […], "asks": […], "volume_ask": …, "volume_bid": …, "symbol": …, "event": "market_data", "price": …, "event_time": "…Z"},"offset":N}}}`.

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
| `internal/exchange/okx` | okx: frames, protocol replies, REST, what NiFi adds |
| `internal/exchange/centrifugo` | the Centrifugo protocol shared by nobitex and bitpin: connect, subscribe, ping, push envelope |
| `internal/exchange/nobitex` | nobitex: its book frame |
| `internal/exchange/bitpin` | bitpin: its book frame |
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
