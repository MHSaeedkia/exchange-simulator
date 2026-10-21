# TODO

## Step 1 — project + shared core + okx (2026-10-21)

- [x] New project + git; `CLAUDE.md` and the related memory copied from lpa-core
- [x] load-generator code moved in and reshaped: exchange contract, okx package, one feed per exchange, start/stop/settings per exchange, per-exchange report
- [x] TLS on the real host names with our own test CA (`-gen-certs`, made on start)
- [x] `docs/REDIRECT.md` + `deploy/docker-compose.yml` (network aliases)
- [x] Tests green; live check with a NiFi-like TLS client. See [[build-log]]
- [ ] Run kafka mode in the new layout against a stack (Docker was down)
- [ ] Build the Docker image once; run it with `deploy/docker-compose.yml`; check `docker exec nifi getent hosts ws.okx.com`
- [ ] Run the redirect against NiFi (needs steps 3 and 5 of REDIRECT.md settled)

## Exchanges — one step each, from the NiFi processor-group export (read → memory → ask → build)

- [ ] **okx (8)** — export still needed for: how NiFi trusts TLS, the REST proxy setup, the real REST URL
- [ ] nobitex (1)
- [ ] bitpin (2)
- [ ] wallex (3) — server PING every 20 s, cuts at 30 min, 51-subscribe limit: decide which of these to copy
- [ ] ramzinex (4)
- [ ] bitget (5)
- [ ] bybit (6)
- [ ] ompfinex (7)
- [ ] lbank (9)

For each: a block in `config.known` + `.env.example` + an alias line in `deploy/docker-compose.yml`, a package under `internal/exchange/`, a case in `main.newExchange`, tests against `docs/sample-raw-data.md`, a section in [[exchange-protocols]].

## Later (ask the user first)

- [ ] Mock ordering for the exchanges that support it (place an order → result on the exchange's private WS) — exchanges not chosen yet
- [ ] kafka mode: answer `snapshot_request` from `control-plane`
- [ ] End-to-end latency per stage (from `pipeline_timings`, as lpa-core's latency-monitor)
- [ ] A stage plan in `.env` for repeatable runs; auto stop at a lag/latency limit
- [ ] Updates that add/remove levels and move prices
