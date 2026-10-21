---
name: origin
description: Where exchange-simulator came from (lpa-core's load-generator, 2026-10-08) and the decisions and measurements carried over from it — read before changing the feed, the rate logic or the report
metadata:
  type: project
---

# Origin: lpa-core's `load-generator/` (2026-10-08)

exchange-simulator started as `load-generator/` on lpa-core branch `feat/load-generator`
(commit `537a0af`, kept in lpa-core, not merged). That tool was a fake OKX only. On
2026-10-21 the user's manager asked for it as a complete project of its own, for all 9
exchanges — see [[project-scope]].

**Goal (user):** find how much load the system (NiFi → Kafka → Flink) takes. Send exchange
frames at a known rate (records per period, period in ms), change rate and book size at
runtime, and get system numbers per stage. Everything from `.env`, nothing hard coded. Laid
out like lpa-console: module = folder name, `internal/domain` for rules, adapters named
after what they talk to, interfaces in the consumer, Options structs, vendored, Dockerfile
with a test stage.

## Decisions the user made (do not re-ask)

- **It acts EXACTLY like the real exchange.** After connect it sends nothing until a
  subscribe; unsubscribe stops that market. For okx the message NiFi sends is
  `{"op":"${status:replace('pending-','')}","args":[{"channel":"books","instId":"${market}"}]}`.
  So in websocket mode the market list comes from the subscribe messages (lpa-console →
  NiFi → simulator), NOT from `.env` and NOT from the DB.
- **kafka mode** (bypass NiFi, write `ex{id}-raw`): no subscribes, so markets = DB rows of
  the exchange with status `subscribe`, re-read every `DB_REFRESH_MS`.
- **A fake REST snapshot too.** NiFi answers a control-plane `snapshot_request` by REST for
  some exchanges; if that call reached the real exchange, REAL prices would mix into the
  fake book.
- **Rate is TOTAL for all markets of an exchange**, round robin. **Book length = two
  settings**: snapshot depth and levels per update. **Stage = any settings change.**
- **Report from the tool itself** (Docker Engine API + a PromQL file), not Grafana only.

## Design calls worth keeping

- **One goroutine owns every book of an exchange and sends every frame** (other goroutines
  send it commands). That is what keeps a market's frames in sequence order — a book-size
  snapshot can never overtake an older update. Do not "optimize" sends onto other goroutines.
- **Sending blocks; it never drops.** A dropped frame breaks the sequence chain → Flink job
  `sequence_gap` → reset + snapshot request, and the test would measure recovery, not load.
  Instead: when more than one period behind, the overdue updates are counted as `missed` and
  the schedule restarts. A stuck client is closed after `WS_WRITE_TIMEOUT_MS`.
- **`sent_per_sec` is per ACTIVE second** (≥ 1 market, records > 0), not per stage second.
  Found live: a 32 s stage with a client connected for 6 s showed 919/s against a real 5000/s.
- okx seqId steps are random (`SEQ_STEP_MIN..MAX`) because real okx steps 3…960 and the
  parser stamps a DYNAMIC jump (`seqId - prevSeqId`); a +1-only fake would not exercise it.
- kafka mode prepends `"id"` + `"simulation"` exactly like NiFi's ReplaceText; without `id`
  the parser drops the frame (`dropped-no-id`). `SIMULATION` defaults to 1.
- The tool never creates Kafka topics (lpa-core rule: only warmup and lpa-console do) — it
  checks the topic exists and stops otherwise.
- `MONITOR_CONTAINERS=none` turns Docker off (an empty value means "default", same `env()`
  rule as lpa-console). Taskmanagers have no fixed container name.
- A PromQL query returning several series is REFUSED, not summed (a sum is wrong for a
  ratio like busy time). `NaN`/`Inf` values are skipped (Flink reports NaN lag when idle,
  and JSON cannot hold NaN).

## Measured with the okx-only version (2026-10-08)

websocket mode, throwaway Go client: nothing before subscribe; 5003/s at a 5000 target and
100020/s at 100000; 25k and 500k updates with the chain unbroken; a stalled client →
blocked → missed counted → closed at the write timeout.

kafka mode against 8 normalizer jobs on a laptop (one taskmanager, 3g):

| target/s | sent/s | missed | Flink busy max (ms/s) | backpressure max | Flink lag max |
|---|---|---|---|---|---|
| 100 | 100.08 | 0 | 41 | 0 | 0 |
| 1 000 | 1 000.35 | 0 | 343 | 9 | 34 |
| 5 000 | 5 003.32 | 0 | 708 | 478 | 49 057 |
| 20 000 | 20 009.46 | 0 | 1000 | 963 | 435 330 |

~1.05 M updates, ZERO rejects. These pipeline numbers say the tool works, not what the real
cluster takes.

## Gotchas met (general)

- A gorilla/websocket read that times out leaves the connection unusable — a test client
  must not "probe for silence" with a deadline and keep reading.
- lpa-core's Flink image must be built after 2026-09-29 (the Postgres driver moved into the
  image), or every DB-reading job fails with `ClassNotFoundException: org.postgresql.Driver`.
- lpa-core's dev compose `TASKMANAGER_MEMORY` default (8g) is bigger than a typical Docker
  Desktop VM (~8 GiB): the taskmanager is OOM-killed (exit 137) every ~40 s. Locally use
  `TASKMANAGER_MEMORY=3g docker compose up -d taskmanager`.
- kafka mode: nobody answers the Flink job's `snapshot_request` (NiFi does in websocket
  mode). Matters only if Flink loses state mid-run; a `book_depth` change (fresh snapshot)
  or a restart recovers it.

## Unverified

- OKX's exact error codes/texts and the REST error HTTP status (200 + code in body) are from
  a partial read of the OKX docs; the `books` checksum rule is from memory.
- Never run through NiFi.
