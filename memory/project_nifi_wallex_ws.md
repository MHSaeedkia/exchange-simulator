---
name: project-nifi-wallex-ws
description: Facts about the Wallex websocket (heartbeat, close rule) and why ConnectWebSocket shows no error when it cannot connect — found while debugging "connected never fires" (2026-09-28)
metadata:
  type: project
---

# Wallex websocket + NiFi ConnectWebSocket (2026-09-28)

**User report:** in the WALLEX group, `ConnectWebSocket` stopped sending anything on
`connected`. It worked before, with no flow change. The other 8 exchange groups still work.
`JettyWebSocketClient` shows as enabled. **Likely root cause found 2026-09-28 (from the NiFi 2.11 source; not yet proven on the
server): a Terminated ConnectWebSocket keeps a dead session factory.** See below.

## What was measured (from a dev Mac, NOT from the NiFi server)

- `wss://api.wallex.ir/ws` answers `101 Switching Protocols`. So the URL still works.
- The first text frame is an Engine.IO-style open message without a numeric prefix:
  `{"sid":…,"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}`.
- The server sends a websocket **control** PING frame (opcode 9, payload `ping`) every 20 s.
  With no PONG, it closes with code 1000 at 30 s. With PONG, the connection stays open.
  Jetty answers control PINGs by itself, so NiFi needs no ping-pong flow for Wallex.

## Why there is no error in the flow

`ConnectWebSocket`'s `failure` relationship only gets a FlowFile when the processor has an
**incoming** FlowFile (dynamic URL/header mode). The WALLEX group has no input to it, so the
`failure → LogAttribute` link can never receive anything. A failed handshake (403/429 from a
CDN/WAF, DNS, firewall) shows up **only** as a bulletin and in `nifi-app.log`.

## Result of the first checks (2026-09-28, from the server)

- Handshake from inside the `nifi` container: **101, works**. So network / IP block is ruled out.
- `nifi-app.log` is full of `PutWebSocket … Failed to send message via WebSocket` at
  `WebSocketMessageRouter.getSessionOrFail`. Meaning: the session id that Redis holds under
  `#{wallex.web_socket.client_id}` belongs to a session that no longer exists. Redis also
  still says `Wallex.conn.state = CONNECTED`, so the `is_connected` guard lets the messages
  through. This is a **result** of the problem, not the cause. The Redis keys are only
  written by the `connected` / `disconnected` events, so a stop or restart that emits no
  `disconnected` leaves both keys stale.
- The runtime ids in the log match `instanceIdentifier` in the export: ConnectWebSocket
  `50a9eea5-6047-3173-f81d-f0faa99e3e8b`, JettyWebSocketClient
  `8d136086-64c5-392e-d20a-04a4cb413bea`, PutWebSocket `d1c078bb-…`.

## The cause (read from NiFi rel/nifi-2.11.0 source)

Every Wallex connect logs `Connected, session=…{CLOSED … c={1011=SERVER_ERROR,
…WebSocketException: RoutingWebSocketListener OPEN method error: null}}`. The TCP/TLS/upgrade
works; NiFi's own open-callback throws, and Jetty closes the socket with 1011.

The chain:
1. `AbstractWebSocketGatewayProcessor.onTrigger` stores `processSessionFactory` **only if
   null**, so the processor keeps the factory from its FIRST run for the life of the instance.
2. `onWebSocketOpen → router.captureSession → connected() → enqueueMessage()` calls
   `processSessionFactory.createSession()` **outside** its try/catch.
3. The factory is a `WeakHashMapProcessSessionFactory`. After `terminateActiveSessions()` its
   `createSession()` throws `TerminatedTaskException`, which has no message → "error: null".
4. `StandardProcessScheduler.terminateProcessor` always calls `state.terminate()` (which
   calls `terminateActiveSessions()`), but **when the active thread count is 0 it returns
   before reloading the processor instance**. The next start replaces the terminated
   lifecycle state, but the processor still holds the dead factory → broken forever.
5. The same dead factory breaks `disconnected` and text messages, so Redis keeps a stale
   session id and `CONNECTED` → PutWebSocket `getSessionOrFail` flood.

Stop/Start, disable/enable of the client service: **do not help** (same processor object).
Fix: restart NiFi, or copy/paste a new ConnectWebSocket and rewire it. Lesson: after using
**Terminate** on any `ConnectWebSocket`/`ListenWebSocket`, restart NiFi or replace the
processor. Not yet proven on the server: grep the logs for `TerminatedTaskException`.

## Outcome (2026-09-28)

User confirmed it is **fixed** after the restart/replace advice (which of the two was not
said). `grep TerminatedTaskException` over `nifi-app*.log` found **nothing**, so the cause
stays "likely, not proven": the logs only reached back to 2026-09-27, when it was already
broken, and Jetty may not log the inner exception.

## 2026-09-30 — one socket accepts only 51 subscribe messages (MEASURED from a dev Mac)

User report: 51 markets (102 subscribe messages, one socket) → some markets never
subscribe; subscribing 2 by 2 after a restart → stops after ~20 markets.

Wallex docs (`developers.wallex.ir/docs/socket-intro`, § محدودیت‌ها): connection cut after
30 min; "max 50 messages per channel"; server PINGs every 20 s, each PONG adds 30 s; max 100
PONGs. The docs do NOT say what happens past 50.

Measured with a raw-socket probe that answers PING with PONG:
- **The server accepts exactly 51 client text messages per connection.** #52+ is silently
  ignored: no error frame, no close, the first 51 channels keep streaming.
- **Repeats count** (49× the same channel, then #50–#51 worked, #52 did not).
- **PONGs do NOT count** (3 PONGs first, then #49–#51 worked, #52 did not).
- **Per connection, not per IP**: two parallel connections each had their own budget.
- **No snapshot on subscribe**: a quiet market sends nothing, so "no data" ≠ "not subscribed".
- Unsubscribe messages were NOT tested; assume they count.

Consequence: max **25 markets per connection** (sell + buy). PutWebSocket reports success on
#52+, so PutSQL writes `subscribe` for a market that gets no data — silent failure.
The user's "20" was never explained (likely extra messages on the same socket).

**Decision (user, 2026-09-30): handle the limit ONLY in NiFi.** lpa-console must not know
NiFi's connection count or the 25-per-connection rule (mixing concerns, and the two copies
would drift). If the admin needs a reason later, NiFi reports it in a general way.

## 2026-09-30 — the user's 5-connection design (reviewed from the flow export)

- 5 `wallex-group-N-socket-connection` groups, each one `ConnectWebSocket` with client id
  `#{wallex.web_socket.client_id}.N`, all on the SAME `JettyWebSocketClient`.
- Redis keys per connection: `Wallex.conn.N.state`, `<client_id>.N` (session).
- A market's connection is `exchange_markets.id % 5` — in `RouteOnAttribute` (subscribe) and
  in each group's disconnect SQL (`AND (id % 5) = N`). Stateless: no mapping to store, and a
  disconnect resets only that connection's rows. Good idea; keep it.
- Seed data (`02_seed.sql`, ids by insert order) gives 11/11/10/10/11 Wallex rows per group
  → ≤ 22 messages per connection. Server ids may differ — check with
  `SELECT id % 5, count(*) FROM exchange_markets WHERE exchange_id = 3 AND status <> 'unsubscribe' GROUP BY 1`.

Problems found in that export:
1. **Ordering regression:** the `text message → output port` (inside each group) and
   `port → LookupAttribute` connections have NO prioritizer. The old single group had
   `OldestFlowFileFirstPrioritizer` on the first hop — that is where the swap happens (see
   [[project-nifi-hot-path]]). Wallex has no sequence field, so a swap is invisible downstream.
2. **The `snapshot_request` filter is gone.** The old `RouteOnAttribute` dropped
   `action == snapshot_request`; the new one routes every FlowFile by `id % 5`.
3. **No guard:** a group that passes 25 markets (or churns unsubscribe/subscribe) fails
   silently again.
4. **5 copies of the subscription path and the connected/disconnected path.** Cache keys and
   SQL accept Expression Language, so one copy can serve all connections using
   `${id:mod(5)}` / `${websocket.endpoint.id:substringAfterLast('.')}`.
5. `5` is hard-coded in 6 places; changing it moves almost every market to a new connection.

## Checks given to the user, in this order

1. Handshake from inside the container: `docker exec nifi curl … https://api.wallex.ir/ws`
   (expect 101). The server IP may be blocked while the dev machine is not.
2. `nifi-app.log` grep for `websocket|wallex|<client id>`.
3. Reset: stop ConnectWebSocket → disable/enable JettyWebSocketClient → start.

## 2026-10-03 — review of the 10-connection export

The user moved to **10 connections** (`id % 10`). Fixed since the 5-socket review: prioritizers
(`OldestFlowFileFirst` on all 20 text-message hops), the `snapshot_request` filter (new
`RouteOnAttribute`: `is_snapshot` dropped, `is_subscription` passed, `unmatched` dropped).
Seed gives 5–6 Wallex rows per group (≤ 12 messages), so the limit is far away today.

Still open / new:
1. **Bug:** `wallex-group-1-socket-connection` disconnect SQL still says `(id % 5) = 1` — it
   also resets group 6's rows. Must be `(id % 10) = 1`.
2. **No message-count guard** (Groovy script is still the plain split).
3. Rows can get stuck in `pending-*` silently: `is_connected` `unmatched` and 10-second
   FlowFile expiration drop FlowFiles; PutWebSocket `failure` → empty `LogMessage` at info.
4. Nobody in WALLEX re-sends `pending-subscribe` rows after a reconnect (asked twice, unanswered).
5. Connected path writes `state=CONNECTED` **before** the new session id → short window where a
   FlowFile reads the old session. Write the session first.
6. `10` is hard-coded in 20 places; 10 copies of each sub-group.
7. Not tested: whether Wallex limits connections **per IP** beyond 2.

## 2026-10-03 — MEASURED again, carefully (raw-socket probe from a dev Mac + control connection)

User asked for exact measurements, not the docs. Results (all repeated or controlled):
- **Without PONG:** closed at **30.0 s** (PING at 20.0 s, close code 1000), 3 of 3 runs, also
  with a live subscription.
- **With PONG:** closed at **exactly 1800 s = 30 min** after connect, code 1000, 3 of 3 runs
  (also with live data). PONGs keep it alive only up to 30 min; the 30-min cut is fixed.
- **NiFi answers PING by itself.** Source: NiFi 2.11.0 `RoutingWebSocketListener` does not
  override `onWebSocketPing` (nor its Jetty parents `AbstractAutoDemanding`/`Abstract`), so in
  Jetty 12.1.11 `JettyWebSocketFrameHandlerFactory` leaves `pingHandle` null and
  `JettyWebSocketFrameHandler.onPingFrame` takes the `// Automatically respond.` branch
  (`sendPong`). Measured too: NiFi's unchanged listener class + Jetty 12.1.11 stayed open
  150 s on Wallex (> 30 s).
- **The limit is 51 SUBSCRIBE requests per connection, for its whole life.** Repeats of the
  same channel count. **Unsubscribe does NOT count** (50 unsubscribes, or 30 unsub/sub churn
  cycles, left room). Invalid text ("hello") does not count. **Unsubscribe does NOT give a
  place back** (51 subs, 5 unsubs, next sub ignored). Not a rate limit (2 s spacing) and not
  a time window (5 min wait). Corrects the 2026-09-30 note "assume unsubscribes count".
- **#52+ is ignored silently:** 0 frames for that channel in 3–6 min while a parallel control
  connection got 1,100–1,700 frames for it; no error text, no close, PINGs and the first 51
  channels continue. Unsubscribe itself works (data stops at once).
- For Option B (counter in lpa-console): count 2 per market on SUBSCRIBE only; reset only when
  NiFi opens a new connection (every 30 min).

## 2026-10-03 — Option B started (user decision: the guard lives in lpa-console + Postgres)

Reverses the 2026-09-30 "NiFi only" decision. Table `exchange_sockets` added to
`postgres/01_schema.sql` + seed (see [[db-schema]]). NiFi's only change: on `connected` in each of
the 10 groups, an UPDATE that sets `subscribes_used = 2 × (that socket's rows in subscribe /
pending-subscribe)` and `reset_at = now()` — counting rows, not 0, so the budget already holds the
markets that must be subscribed again after the 30-min cut, whoever re-sends them.
lpa-console NOT started — user wants the plan first.

## 2026-10-03 — NiFi "Control Panel" group (export read) — the HTTP call does NOT send to Wallex

`ListenHTTP` (port `#{control_panel.port}` 8081, Base Path "", Return Code 200, no SSL/auth) →
`RouteOnAttribute` on `restlistener.request.uri` (`/control-plane/subscribe|unsubscribe|market|
exchange-market`, unmatched dropped) → `EvaluateJsonPath` → `PutSQL`. **subscribe/unsubscribe only
run `UPDATE exchange_markets SET status = 'pending-…' WHERE exchange = '${exchange}' AND market =
'${market}'`** — the same write lpa-console already made. ListenHTTP answers 200 as soon as the
FlowFile is queued, before PutSQL. So something ELSE reads `pending-*` rows and feeds WALLEX
`input market` (asked the user what — not seen yet). Consequences:
- lpa-console's HTTP retries cannot double-send to Wallex (they repeat an idempotent UPDATE).
- A failed HTTP call does not mean "not sent": the poller may already have sent lpa-console's
  pending row. So keeping the socket places on NiFi failure is RIGHT (decision kept).
- Old race, not new: poller sends + PutSQL writes `subscribe`, then lpa-console's rollback writes
  the old status → row says unsubscribe while Wallex streams.
Problems seen in this group (reported, not fixed — NiFi is the user's): `${exchange}`/`${market}`
pasted into SQL from an unauthenticated HTTP body (SQL injection, and a `'` in a name breaks it);
`/exchange-market` upsert sets `status = 'unsubscribe'` on conflict (silently "turns off" a live
row while the feed keeps running); PutSQL failure → info LogAttribute, retry self-loops forever.

## 2026-10-03 — "Data Ingestion Controler" group (export read) = the poller that sends subscribes

Answers the open question "who re-sends pending rows": **`QueryDatabaseTableRecord`, every 2 s,
primary node, Custom Query = every `exchange_markets` row in `pending-subscribe`/`pending-unsubscribe`
(all exchanges), NO Maximum-value Columns** → UpdateAttribute `action=subscription` → SplitRecord →
EvaluateJsonPath (id, status, market, name…) → output port → each exchange's `input market`.
Second path: ConsumeKafka `control-plane` → ExecuteSQLRecord (`'${pair_id}'` pasted in) →
`status_is_subscribe` → same port (snapshot requests; no `id` attr; WALLEX drops them).
**Risk for the socket count:** with no max-value column QueryDatabaseTable returns ALL matching rows
on every run (likely; verify in NiFi provenance), so a row still pending after 2 s is SENT AGAIN.
Wallex counts repeats → the console/NiFi-reset count (2 per market) becomes too low → #52 can
happen silently. Happens when the WALLEX path takes > 2 s, or PutWebSocket succeeds but PutSQL fails.
Proposed (waiting for the user): claim rows once (`claimed_at` column + `UPDATE … RETURNING` poller,
retry after 30 s; console + disconnect/connect SQL set it NULL) or a NiFi `DetectDuplicate` before
PutWebSocket.
