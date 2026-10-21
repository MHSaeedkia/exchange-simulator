# TODO

## Step 1 — project + shared core + okx (2026-10-21)

- [x] New project + git; `CLAUDE.md` and the related memory copied from lpa-core
- [x] load-generator code moved in and reshaped: exchange contract, okx package, one feed per exchange, start/stop/settings per exchange, per-exchange report
- [x] TLS on the real host names with our own test CA (`-gen-certs`, made on start)
- [x] `docs/REDIRECT.md` + `deploy/docker-compose.yml` (network aliases)
- [x] Tests green; live check with a NiFi-like TLS client. See [[build-log]]
- [ ] Run kafka mode in the new layout against a stack (Docker was down)
- [ ] Build the Docker image once; run it with `deploy/docker-compose.yml`; check `docker exec nifi getent hosts ws.okx.com`
- [x] Steps 3 and 5 of REDIRECT.md settled from the NiFi flow: NiFi uses JVM `cacerts`; proxied calls go to the simulator's own HTTP proxy (`PROXY_PORT`, a test changes only `proxy_server.host`)
- [ ] Run the redirect against NiFi (aliases + CA in `cacerts` + `proxy_server.host` → `exchange-simulator`)

## Exchanges — one step each, from the NiFi flow in `../NIFI-Version-Controller` (read → memory → ask → build)

- [x] **okx (8)** — flow read (facts in [[exchange-protocols]]); matches what step 1 built; the proxy it needs is built (step 1b)
- [x] nobitex (1) — built with unit tests; live check waits for the end (see below)
- [x] bitpin (2) — built with unit tests on the shared `centrifugo` package; live check at the end
- [x] nobitex/bitpin small real rules added (3501 closes, error 105, offset in the ack, bitpin price padding)
- [x] wallex (3) — all real rules copied (user: exactly as real): open message, no replies, 51 subscribes per connection, PING 20 s / close after 30 s without PONG, close at 30 min; live check at the end
- [x] ramzinex (4) — WS + REST, its Centrifugo differences; the 3012 no-pong close added to all Centrifugo exchanges; live check at the end
- [ ] bitget (5)
- [ ] bybit (6)
- [ ] ompfinex (7)
- [ ] lbank (9)

For each: a block in `config.known` + `.env.example` + an alias line in `deploy/docker-compose.yml`, a package under `internal/exchange/`, a case in `main.newExchange`, tests against `docs/sample-raw-data.md`, a section in [[exchange-protocols]].

## After all exchanges (user, 2026-10-21: live checks at the end)

- [ ] Live check every exchange with a NiFi-like client, then the whole redirect with NiFi
- [ ] Decide the `EXCHANGES_RUNNING` default (today `okx` only: the others start stopped)

## Later (ask the user first)

- [ ] Proxy: forward hosts the simulator does not play to the real proxy (today they are refused during a test)

- [ ] Mock ordering for the exchanges that support it (place an order → result on the exchange's private WS) — exchanges not chosen yet
- [ ] kafka mode: answer `snapshot_request` from `control-plane`
- [ ] End-to-end latency per stage (from `pipeline_timings`, as lpa-core's latency-monitor)
- [ ] A stage plan in `.env` for repeatable runs; auto stop at a lag/latency limit
- [ ] Updates that add/remove levels and move prices
