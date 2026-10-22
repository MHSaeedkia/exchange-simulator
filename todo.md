# TODO

## Step 1 — project + shared core + okx (2026-10-21)

- [x] New project + git; `CLAUDE.md` and the related memory copied from lpa-core
- [x] load-generator code moved in and reshaped: exchange contract, okx package, one feed per exchange, start/stop/settings per exchange, per-exchange report
- [x] TLS on the real host names with our own test CA (`-gen-certs`, made on start)
- [x] `docs/REDIRECT.md` + `deploy/docker-compose.yml` (network aliases)
- [x] Tests green; live check with a NiFi-like TLS client. See [[build-log]]
- [x] Run kafka mode in the new layout against a stack
- [x] Build the Docker image once; run it with `deploy/docker-compose.yml`; check `docker exec nifi getent hosts ws.okx.com`
- [x] Steps 3 and 5 of REDIRECT.md settled from the NiFi flow: NiFi uses JVM `cacerts`; proxied calls go to the simulator's own HTTP proxy (`PROXY_PORT`, a test changes only `proxy_server.host`)
- [x] Run the redirect against NiFi (aliases + CA in `cacerts` + `proxy_server.host` → `exchange-simulator`)

## Exchanges — one step each, from the NiFi flow in `../NIFI-Version-Controller` (read → memory → ask → build)

- [x] **okx (8)** — flow read (facts in [[exchange-protocols]]); matches what step 1 built; the proxy it needs is built (step 1b)
- [x] nobitex (1) — built with unit tests; live check waits for the end (see below)
- [x] bitpin (2) — built with unit tests on the shared `centrifugo` package; live check at the end
- [x] nobitex/bitpin small real rules added (3501 closes, error 105, offset in the ack, bitpin price padding)
- [x] wallex (3) — all real rules copied (user: exactly as real): open message, no replies, 51 subscribes per connection, PING 20 s / close after 30 s without PONG, close at 30 min; live check at the end
- [x] ramzinex (4) — WS + REST, its Centrifugo differences; the 3012 no-pong close added to all Centrifugo exchanges; live check at the end
- [x] bitget (5) — books50, real replies, the 2-min no-ping cut (NiFi gets cut, as on real bitget); live check at the end
- [x] bybit (6) — WS + REST (through the proxy), real replies and the 10-arg rule; no idle cut (11-min probe); live check at the end
- [x] ompfinex (7) — deltas-only WS, REST depth as the only snapshot; the unasked-`{}` 3501 rule fixed for all Centrifugo; live check at the end
- [x] lbank (9) — fdepth, plain-text ack, its ping/pong and the 6-missed-pings close, TS in UTC+8 as real; live check at the end

For each: a block in `config.known` + `.env.example` + an alias line in `deploy/docker-compose.yml`, a package under `internal/exchange/`, a case in `main.newExchange`, tests against `docs/sample-raw-data.md`, a section in [[exchange-protocols]].

## After all exchanges (user, 2026-10-21: live checks at the end)

- [x] Kafka mode against real Kafka + Flink: all 9, 0 rejects
- [x] Image build, DNS aliases, CA in NiFi cacerts, flow import, proxy param, all 9 connected + subscribed through real NiFi
- [ ] Clean NiFi run measurement (blocked: Docker VM 8 GB → OOM kills of Kafka/taskmanager; user to raise memory or accept smaller Kafka heap)
- [x] Simulator restart vs Flink state: books start from the clock (SEQ_START + ms since 2026 × step)
- [x] `EXCHANGES_RUNNING` default = all 9 (user, 2026-10-22, from their .env)
- [ ] Tell lpa-core (user's call): real lbank `TS` is UTC+8 but LBankParser reads it as UTC → ex9 event times 8 h ahead
- [ ] Tell NiFi owner (user's call): LBANK ConnectWebSocket uses BITGET's client id; lbank's 59-byte ack is echoed back by the ≤60-byte ping path

## Later (ask the user first)

- [ ] Proxy: forward hosts the simulator does not play to the real proxy (today they are refused during a test)

- [ ] Mock ordering for the exchanges that support it (place an order → result on the exchange's private WS) — exchanges not chosen yet
- [ ] kafka mode: answer `snapshot_request` from `control-plane`
- [ ] End-to-end latency per stage (from `pipeline_timings`, as lpa-core's latency-monitor)
- [ ] A stage plan in `.env` for repeatable runs; auto stop at a lag/latency limit
- [ ] Updates that add/remove levels and move prices
