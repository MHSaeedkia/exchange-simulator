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
- **How an exchange step goes:** the user sends that exchange's NiFi processor group export,
  one at a time. Read it, write the needed facts into [[exchange-protocols]], ask about
  anything unclear BEFORE writing code, then build (tests first), live check, update memory +
  `todo.md`, commit. The raw export files are NOT committed unless the user says so.
- Runs **in Docker beside NiFi**.
- lpa-core's `feat/load-generator` branch is kept; `Tibobit/crypto-mock-generator` (an older,
  Kafka-only mock with templates for 8 exchanges) is NOT touched and NOT replaced.
- Mock orders: decide later.
