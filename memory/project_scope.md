---
name: project-scope
description: What exchange-simulator must do and the user's decisions of 2026-10-21 (all 9 exchanges, real host names via DNS + own CA, stopped = silent, step by step from NiFi exports) — read first
metadata:
  type: project
---

# Scope (user + manager, 2026-10-21)

1. **All 9 exchanges** — nobitex (1), bitpin (2), wallex (3), ramzinex (4), bitget (5),
   bybit (6), ompfinex (7), okx (8), lbank (9) — each with its own real protocol and frames.
   Start with all or some; at runtime start/stop an exchange and change its rate and book size.
2. **Report per exchange**, same idea as the okx version ([[origin]]).
3. **Real paths, no NiFi flow change.** NiFi keeps the real URLs (e.g.
   `wss://ws.okx.com:8443/ws/v5/public`); the network sends those calls to the simulator.
   A written procedure for this (`docs/REDIRECT.md`).
4. **Mock ordering later** (place an order → see the result on the exchange's WS, for the
   exchanges that support it). NOT built now; the design only keeps room for it.

## User decisions (do not re-ask)

- Name **exchange-simulator** (own git repo next to lpa-core).
- Redirect = **DNS + our own test CA**: inside the docker network the real exchange host
  names resolve to the simulator; it shows TLS certs for those names signed by our CA; the CA
  goes into NiFi's truststore. NiFi flows unchanged.
- A **stopped** exchange is **silent**: it keeps the connection and answers the protocol
  (acks, ping), but sends no market data. No pass-through to the real exchange.
- **kafka mode kept** for all 9 (writes `ex{id}-raw` with what NiFi adds).
- **Step by step.** Step 1 = project + shared core + okx + TLS/redirect. Then one exchange per
  step; the user checks each before the next.
- **Where the NiFi flow comes from (user, 2026-10-21):** read it yourself from the sibling
  repo `../NIFI-Version-Controller` (NiFi's version control, file
  `default/Data-Collection.json`, one big versioned flow). The user no longer sends exports.
  `git pull`/`git log` there first — it changes. The live groups are under
  `Data Ingestion Public`; `Data Ingestion Private` is an all-disabled old copy. The file holds
  real parameter values (e.g. the proxy address): never copy those into this repo or memory.
- **How an exchange step goes:** read that exchange's group in the flow. Read it, write the needed facts into [[exchange-protocols]], ask about
  anything unclear BEFORE writing code, then build (tests first), live check, update memory +
  `todo.md`, commit. Nothing from the flow file is committed unless the user says so.
- **Live checks after all exchanges (user, 2026-10-21):** "letls do all tests after all" —
  build the exchanges one after another with unit tests (TDD still holds), and run the live
  / NiFi checks for all of them at the end, not per exchange.
- **"Exactly the same as real" (user, 2026-10-21, on wallex):** copy the real exchange's
  connection rules too — wallex's 20 s PING / 30 s pong timeout, the 30-min cut and the
  51-subscribe limit were all asked for. Apply the same standard to every exchange.
- Runs **in Docker beside NiFi**.
- lpa-core's `feat/load-generator` branch is kept; `Tibobit/crypto-mock-generator` (an older,
  Kafka-only mock with templates for 8 exchanges) is NOT touched and NOT replaced.
- Mock orders: decide later.
