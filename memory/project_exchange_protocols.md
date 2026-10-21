---
name: exchange-protocols
description: Per-exchange fact sheet for the 9 exchanges (feed type, endpoints, subscribe text, frame shape, market key, sequence, what NiFi adds, snapshot answer, limits) — endpoints/proxy/TLS of all 9 and the okx details read from the NiFi flow repo; other gaps marked
metadata:
  type: project
---

# The 9 exchanges, as NiFi and lpa-core's parsers see them

Built 2026-10-21 from lpa-core (`sample-raw-data.md` → `docs/sample-raw-data.md` here, the
memory notes, and the parsers in `flink/normalizer/job-parser/`). **The NiFi flow is not in
lpa-core**, so everything marked *NOT KNOWN* waits for that exchange's processor-group export
(the user sends them one by one — see [[project-scope]]). *(inferred)* = not read anywhere.

## Common to all

- NiFi adds `id` (a new UUID) and `simulation` (0/1) at the front of every object-root payload
  (`ReplaceText` `^\s*\{` → `{"id":"${uuid}","simulation":0,`); wallex (array root) gets them as
  a 3rd element `{"simulation":N,"id":"…"}`. No `id` → the parser drops the record.
- REST bodies also get `"action":"snapshot"` and `"pair":"<DB market string>"` (at the END of
  the body for okx).
- NiFi publishes to `ex{exchange_id}-raw`. Market subscribe/unsubscribe come from lpa-console
  through Postgres `pending-*` rows that a NiFi poller sends into each exchange group.
- Snapshot requests: see [[snapshot-requests]].
- **TLS trust (from the flow, 2026-10-21):** there is NO SSLContextService anywhere in the
  flow; no JettyWebSocketClient and no InvokeHTTP sets one. So NiFi uses the JVM default
  truststore (`cacerts`) for every exchange *(the JVM-default part is inferred from how Jetty
  and OkHttp behave; lpa-core's NiFi image sets no `javax.net.ssl.trustStore`)*.
- **Proxy (from the flow):** one HTTP proxy, set by the parameters `proxy_server.host` /
  `proxy_server.port`, is used by: **okx WS + REST, lbank WS + REST, bybit REST**. All other
  WS and REST calls go direct. A proxied call is NOT caught by our DNS aliases (the proxy
  resolves the name on its own machine) — see [[redirect]].
- **Every text frame goes to Kafka**, acks and pongs too (`text message` → LookupAttribute →
  ReplaceText → port). A non-object frame (`pong`) gets no `id` and the parser drops it.
- Kafka record = the FlowFile content only: no key, no headers.
- WS client limits (all groups): Max Text Message Size **64 kb**, Idle Timeout 0, Connection
  Attempt Count 10. A frame over 64 kb would make Jetty close the socket *(inferred)*.

| # | exchange | feed | known endpoint | market key (DB) | sequence | event time |
|---|---|---|---|---|---|---|
| 1 | nobitex | WS Centrifugo, every push a full snapshot (the REST branch is gone from NiFi) | `wss://ws.nobitex.ir/connection/websocket` | `BTCUSDT` (WS channel `public:orderbook-BTCUSDT`) | WS `pub.offset`, jump 0; REST null | `lastUpdate` epoch ms (number) |
| 2 | bitpin | WS Centrifugo, every push a full snapshot (the REST branch is gone from NiFi) | `wss://centrifugo.bitpin.ir/connection/websocket` | `BTC_USDT` (underscore, REST `pair` must match) | WS `pub.offset`, jump 0; REST null | WS ISO string, REST epoch ms number |
| 3 | wallex | WS only; each frame = full snapshot of ONE side | `wss://api.wallex.ir/ws` | `BTCUSDT` (`BTCUSDT@buyDepth` / `@sellDepth`) | none (null) | none (processing time) |
| 4 | ramzinex | WS Centrifugo, full snapshot both sides (+ a REST branch NiFi never uses today) | `wss://websocket.ramzinex.com/websocket` | numeric `12` (channel `orderbook:12`) | `pub.offset`, jump 0 | none (processing time) |
| 5 | bitget | WS `books50`, snapshot only | `wss://ws.bitget.com/v2/ws/public` | `BTCUSDT` (`arg.instId`) | `data[i].seq` (int), jump 0 | inner `ts` string ms |
| 6 | bybit | WS `orderbook.50.{sym}` snapshot+delta, REST `/v5/market/orderbook` (through the proxy) | WS `wss://stream.bybit.com/v5/public/spot`, REST `https://api.bybit.com` | `BTCUSDT` (`data.s` / `result.s`) | WS `data.u`, jump 1; snapshot with `u==1` = restart (null); REST null | `cts` |
| 7 | ompfinex | REST snapshot + Centrifugo WS delta (captured 2026-10-21) | `wss://stream.ompfinex.com/stream`, REST `https://api.ompfinex.com/v1` | numeric `14` (channel `public-market:r-depth-14`) | REST `data.lastUpdateId`, jump 0; WS `u`, jump `u−U` | REST `data.time` µs ÷ 1000; WS processing time |
| 8 | okx | WS `books` snapshot+update, REST snapshot | WS `wss://ws.okx.com:8443/ws/v5/public`; REST `https://www.okx.com/api/v5/market/books` — both through the proxy | `BTC-USDT` (`arg.instId`) | `seqId`, jump `seqId−prevSeqId`; snapshot jump 0; REST null | `ts` string ms |
| 9 | lbank | WS `fdepth`, snapshot only (+ a REST branch never used today) | `wss://www.lbank.com/old-wss/ccws/ws/V3`, REST `https://api.lbkex.com` — both through the proxy | `btc_usdt` (lower case, underscore) | none (null) | `TS` ISO without zone, read as UTC |

## Per exchange — details

- **nobitex (1)**: WS frame `{"push":{"channel":"public:orderbook-BTCUSDT","pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}`, levels `[str,str]`, 24/side. Built in step 2 — see § nobitex below.
- **bitpin (2)**: WS channel `orderbook:BTC_USDT`; `data` = `bids, asks, volume_ask, volume_bid, symbol, event:"market_data", price`, `event_time`; 50 levels. Built in step 3 — see § bitpin below.
- **wallex (3)**: built in step 4 — see § wallex below. First frame `{"sid":…,"upgrades":[],"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}`; server sends a control PING every 20 s, closes at 30 s without PONG; cuts EVERY connection at exactly 30 min. Frame `["BTCUSDT@buyDepth",[{"price":62525.04,"quantity":0.000451,"sum":…}]]` (numbers). 2 subscribes per market (buy + sell); no snapshot on subscribe. **Limit: 51 subscribe messages per connection for its whole life**, #52+ ignored silently. Subscribe text NOT KNOWN. Facts in [[project-nifi-wallex-ws]].
- **ramzinex (4)**: `data.buys`/`data.sells` 7-element numeric arrays `[price,qty,notional,false,null,int,epochMs]`, BOTH sides sorted descending. Built in step 5 — see § ramzinex below.
- **bitget (5)**: `{"action":"snapshot","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"},"data":[{asks,bids,"ts":"…","seq":N,"pseq":0}],"ts":N}`. A snapshot answer must be a resubscribe (a REST body would be dropped). Built in step 6 — see § bitget below.
- **bybit (6)**: `{"topic":"orderbook.50.BTCUSDT","ts":…,"type":"snapshot|delta","data":{"s","b","a","u","seq"},"cts":…}`; an unchanged side on a delta is `[]`. More than 10 args in one subscribe → `{"success":false,"ret_msg":"args size >10"}` and then NOTHING. REST body under `result`. Built in step 7 — see § bybit below.
- **ompfinex (7)**: built in step 8 — see § ompfinex below.
- **okx (8)**: built in step 1 (from the okx-only version). Subscribe/unsubscribe `{"op":"subscribe|unsubscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}`; ack `{"event":"subscribe","arg":{…},"connId":…}`; errors 60012 / 60018; text `ping` → `pong`. Frame `{"arg":{…},"action":"snapshot|update","data":[{asks,bids,ts,checksum,prevSeqId,seqId}]}`, 4-element levels, 400 levels on a snapshot. REST `{"code":"0","msg":"","data":[{asks,bids,ts,seqId}]}`.
- **lbank (9)**: built in step 9 — see § lbank below.

## Filled in from NiFi exports

Source: the NiFi flow in the sibling repo (see [[project-scope]]), group
`Data Ingestion Public/<EXCHANGE>`. Parameter values are in the `CONFIG` parameter context.

**Endpoints of all 9 (parameters, 2026-10-21)** — WS URL / REST call / proxy:

| # | WS | REST call (InvokeHTTP, snapshot requests only) | proxy |
|---|---|---|---|
| 1 nobitex | `wss://ws.nobitex.ir/connection/websocket` | none in the live group | no |
| 2 bitpin | `wss://centrifugo.bitpin.ir/connection/websocket` | none in the live group | no |
| 3 wallex | `wss://api.wallex.ir/ws` | none | no |
| 4 ramzinex | `wss://websocket.ramzinex.com/websocket` | `https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/${pair}/buys_sells` | no |
| 5 bitget | `wss://ws.bitget.com/v2/ws/public` | none in the live group | no |
| 6 bybit | `wss://stream.bybit.com/v5/public/spot` | `https://api.bybit.com/v5/market/orderbook?category=spot&symbol=${pair}&limit=50` | REST only |
| 7 ompfinex | `wss://stream.ompfinex.com/stream` | `https://api.ompfinex.com/v1/market/${pair}/depth?limit=50` | no |
| 8 okx | `wss://ws.okx.com:8443/ws/v5/public` | `https://www.okx.com/api/v5/market/books?instId=${pair}&sz=100` | WS + REST |
| 9 lbank | `wss://www.lbank.com/old-wss/ccws/ws/V3` | `https://api.lbkex.com/v2/depth.do?symbol=${pair}&size=50` | WS + REST |

(`Data Ingestion Private` is an older copy with every processor DISABLED — ignore it.)

### okx (8) — read from the flow 2026-10-21

- **Subscribe / unsubscribe** (ReplaceText, pretty-printed JSON with newlines):
  `{"op": "${status:replace('pending-', '')}", "args": [{"channel": "books", "instId": "${market}"}]}`
  — one market per message. Sent by PutWebSocket only while Redis `OKX.conn.state` =
  `CONNECTED`; on PutWebSocket success, PutSQL sets the row to `subscribe` / `unsubscribe`.
  **NiFi never reads the ack** (it only goes to Kafka with the data).
- **On every `connected`**: session id → Redis, state `CONNECTED`, then
  `UPDATE exchange_markets SET status='pending-subscribe' WHERE status='subscribe' AND
  exchange = OKX` → the poller (every 2 s) re-sends every subscribe. So a closed socket heals
  itself: NiFi reconnects and resubscribes all markets.
- **No ping/pong flow** in the okx group (NiFi never sends `ping`).
- **Data path**: `text message` → LookupAttribute (`exchange_id`) → ReplaceText `^\s*\{` →
  `{"id":"${uuid}","simulation":0,` → UpdateAttribute (`kafak_topic_raw_data=ex${exchange_id}-raw`,
  attributes only) → PublishKafka (transactions on, 1 task). OldestFlowFileFirst on the hops.
- **Snapshot request** (`action == snapshot_request` from the controller group) → InvokeHTTP
  GET `#{okx.api_url}/api/v5/market/books?instId=${pair}&sz=#{okx.depth_limit}` (= `sz=100`),
  **through the proxy**, 4 tasks, no SSL service → JoltTransformJSON `default`
  `{"action":"snapshot","pair":"${pair}"}` (added at the end) → same LookupAttribute/ReplaceText
  path → ex8-raw.
- TLS: JVM default truststore (see Common). Proxy: WS AND REST (see Common).

### nobitex (1) — read from the flow + measured live 2026-10-21

**NiFi side (group `NOBITEX`):**
- On `connected`: state `CONNECTED` → session id to Redis → sends **`{"id":1,"connect":{}}`**.
- Subscribe / unsubscribe: **`{"id":1,"${status:replace('pending-', '')}":{"channel":"public:orderbook-${market}"}}`**
  (always id 1). PutSQL sets the row on PutWebSocket success; the ack is never read.
- **Ping path:** a text frame of **10 bytes or less** (`fileSize:le(10)`) is a ping → NiFi
  sends `{}` back and does NOT publish it. Bigger frames → id/simulation → ex1-raw (acks too).
- On `disconnected` (not on connected, unlike okx): `subscribe` rows → `pending-subscribe`,
  so the poller resubscribes once the socket is back.
- `snapshot_request` is dropped (`is_snapshot` auto-terminated). **No REST call** in the live
  group (the old REST branch was deleted).
- WS direct, no proxy → caught by the DNS alias.

**Real nobitex, measured from a dev Mac (raw socket, ~1 min):**
- connect reply `{"id":1,"connect":{"client":"<uuid>","version":"6.9.3 OSS","ping":25,"pong":true}}`.
- subscribe reply `{"id":1,"subscribe":{"recoverable":true,"epoch":"<8 letters>","offset":<last>,"positioned":true}}`
  (no `offset` key for a channel that has none yet). **No snapshot on subscribe**: first push
  on the normal beat (~1 s later).
- unsubscribe reply `{"id":1,"unsubscribe":{}}` (also when not subscribed).
- second subscribe to the same channel → `{"id":1,"error":{"code":105,"message":"already subscribed"}}`.
- unknown market (`public:orderbook-NOPEUSDT`) → accepted (ack, no data).
- subscribe before connect → socket closed, code **3501 "bad request"**.
- server ping `{}` (first at ~18 s, then every 25 s); client answers `{}`.
- pushes ~1/s per market, ~1.2 KB, **24 levels per side**, offset **+1 per push**, keys
  exactly `push.channel`, `push.pub.{data,offset}`, `data.{asks,bids,lastTradePrice,lastUpdate}`.

### bitpin (2) — read from the flow + measured live 2026-10-21

**NiFi side (group `BITPIN`):** the SAME flow as NOBITEX (connect `{"id":1,"connect":{}}`,
≤10-byte frames = ping → `{}` back, reset to `pending-subscribe` on disconnect,
`snapshot_request` dropped, no REST, no proxy). Only the channel differs:
**`{"id":1,"${status:replace('pending-', '')}":{"channel":"orderbook:${market}"}}`**.

**Real bitpin, measured from a dev Mac:**
- Same Centrifugo replies as nobitex; connect says **`"version":"6.9.2 OSS"`**.
- Live-confirmed: `trades:BTC_USDT` → `{"id":1,"error":{"code":102,"message":"unknown channel"}}`;
  repeat subscribe → error 105; unknown market `orderbook:NOPE_USDT` accepted; no snapshot on subscribe.
- `{}` ping first at ~19 s. Pushes irregular (several per second at times), ~3 KB.
- **`data` is written Python-style**: `{"bids": [["84264.78", "0.00713113"], …], "asks": […],
  "volume_ask": "2.52660052", "volume_bid": "2.33289293", "symbol": "BTC_USDT", "event":
  "market_data", "price": "84300.00", "event_time": "2026-10-21T19:14:48.495001Z"}` — space after
  every `:` and `,` inside `data` only (the envelope has none). 50 levels per side, bids first.
- **`price` is now a STRING** (`"84300.00"`); the July sample had a number. Parser ignores it.
- Prices padded to 2 decimals, quantities 8 decimals.

### wallex (3) — read from the flow + measured live 2026-10-21

**NiFi side (group `WALLEX`):** 10 sub-groups `wallex-group-N-socket-connection` (one
ConnectWebSocket each, client id `#{wallex.web_socket.client_id}.N`, all on ONE
JettyWebSocketClient → the same URL) + 10 `wallex-goup-N-subscription`. A market goes to
connection **`id % 10`**. Per market TWO messages:
**`["${status:replace('pending-', '')}", {"channel": "${market}@buyDepth"}]`** and the same
with `@sellDepth` (note the spaces). On `connected`: `exchange_sockets.subscribes_used` reset
to 2 × that socket's rows, state/session to Redis. On `disconnected`: that socket's
`subscribe` rows → `pending-subscribe`. Text frames of all 10 → LookupAttribute →
**ReplaceText `\]\s*$` → `, {"simulation": 0, "id": "${uuid}"} ]`** (3rd array element) →
ex3-raw. `snapshot_request` dropped. No REST, no proxy.

**Real wallex, measured from a dev Mac 2026-10-21** (adds to [[project-nifi-wallex-ws]]):
- First frame `{"sid":"<uuid>","upgrades":[],"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}`.
- **No reply at all** to subscribe, unsubscribe, unknown channel (`NOPEUSDT@buyDepth`, `BTCUSDT`),
  or bad text (`hello`). Data starts ~0.15 s after a subscribe on an active market.
- Frame `["BTCUSDT@sellDepth",[{"price":84356.47,"quantity":0.00035696,"sum":30.1118855312},…]]`:
  **50 levels**, JSON numbers, no trailing zeros, no spaces, **sum = price × quantity exactly**
  (50/50). ~2.9 KB. BTCUSDT: ~5 buy + ~2 sell frames per second.
- Control PING payload `ping` at 20 s.

### Centrifugo rules (nobitex AND bitpin, probed 2026-10-21)

- Before connect: anything but `{"id":N,"connect":{…}}` with N>0 → close **3501 "bad request"**
  (even `{}`, even `{"connect":{}}` with no id).
- After connect → 3501: bad JSON (`hello`), `[]`, unknown command (`{"id":2,"foo":{}}`), a
  second connect, any command with no id (`{"subscribe":…}`, `{"unsubscribe":…}`), and
  `{"id":3,"subscribe":{}}` (no channel). `{}` after connect is the pong: fine.
- `{"id":4,"subscribe":{"channel":"<own prefix>"}}` (no market) → acked with NO offset, no data.
- Other namespace (nobitex `orderbook:`, bitpin `public:orderbook-`, `trades:…`) → error 102.
- Repeat subscribe → 105; unsubscribe not subscribed → `{"id":N,"unsubscribe":{}}`.
- Inferred: unsubscribe with no channel closes like subscribe with none.
- **An unasked-for `{}` after connect → close 3501** (nobitex, bitpin, ramzinex, ompfinex all
  checked). `{}` is a pong ONLY while a ping is waiting for its answer.
- **No pong → close 3012 "no pong"**: 8 s after the `{}` ping on nobitex and bitpin, 10 s on
  ramzinex. The FIRST ping comes at a varying time (13.6–21.2 s seen), then every 25 s
  (the simulator pings on a fixed 25 s beat — not copied).

### ramzinex (4) — read from the flow + measured live 2026-10-21

**NiFi side (group `RAMZINEX`):** the NOBITEX-style Centrifugo flow (connect `{"id":1,"connect":{}}`,
≤10-byte frames = ping → `{}`, reset on disconnect), channel **`orderbook:${market}`** (numeric
id), no proxy. Unlike nobitex/bitpin it still has a **snapshot branch**: `snapshot_request` →
InvokeHTTP `GET https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/${pair}/buys_sells`
→ Jolt `default` action/pair → ex4-raw. lpa-core never sends ramzinex a snapshot request, and
RamzinexParser has no REST branch (such a body would be dropped).

**Real ramzinex (probed from a dev Mac):**
- connect reply **without `version`**: `{"id":1,"connect":{"client":"<uuid>","ping":25,"pong":true}}`.
- subscribe ack epoch is **number-like** (`"1790133057"`; unknown channels get the current unix time).
- **any namespace accepted** (`public:orderbook-12` → ack, no data) — no error 102.
- repeat → 105; bad JSON / `{}` before connect / subscribe without channel → 3501; no pong → 3012 after 10 s.
- push ~6 KB, every ~2–3 s for market 12, **50 levels per side, BOTH descending**; level
  `[84048,0.000042,3.530016,false,null,10,1792611279396]` — notional is a float product with
  float artifacts (`1373.3777799999998`; only ~half equal Python's p*q, so computed in some
  other order); el6 8…90 (meaning unknown); el7 per-level epoch ms, up to ~25 min old.
- REST 200: `{"data":{"buys":[…],"sells":[…]},"status":0}` (same arrays); unknown market →
  **400 `{"error":"Order book not found for pair_id"}`** (the simulator has every market: not copied).

### bitget (5) — read from the flow + measured live 2026-10-21

**NiFi side (group `BITGET`):** subscribe/unsubscribe
**`{"op":"${status:replace('pending-', '')}","args":[{"channel":"books50","instType":"SPOT","instId":"${market}"}]}`**.
**NiFi sends NO ping** (and drops ≤10-byte frames, i.e. `pong`). On `disconnected`: rows →
`pending-subscribe`. `snapshot_request` dropped. No REST, no proxy.

**Real bitget (probed from a dev Mac):**
- ack per arg `{"event":"subscribe","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"}}`
  (arg keys reordered); repeat subscribe → acked again; unsubscribe of a market not subscribed → acked.
- unknown instId → `{"event":"error","arg":{…},"code":30001,"msg":"instType:SPOT,channel:books50,instId:NOPEUSDT,precision:null doesn't exist","op":"subscribe"}` (not copied: every market exists here);
  other channel → `…"code":30016,"msg":"Param error","op":"subscribe"}`; text it cannot read
  (`hello`, `{"op":"login"}`) → `{"event":"error","code":30002,"msg":"Unrecognized request:<text>"}`;
  `{"op":"subscribe","args":[]}` → nothing; `ping` → `pong`.
- frames ~8/s on BTCUSDT, first one on the normal beat after the ack; 50/50 levels; quantities
  without trailing zeros; outer `ts` = inner + 2 ms.
- **Idle cut: no `ping` for ~2 min → TCP dropped, no close frame (1006).** Cut at 126 s and
  129.5 s after connect with no ping (a subscribe at 60 s did NOT reset it), and 145 s after a
  ping at 60 s (205 s). Modelled as: limit 120 s, checked every 30 s from a random phase.
  **Consequence: real bitget cuts NiFi's connection about every 2 minutes** (NiFi never pings);
  NiFi reconnects and resubscribes.

### bybit (6) — read from the flow + measured live 2026-10-21

**NiFi side (group `BYBIT`):** subscribe **`{"op":"${status:replace('pending-', '')}","args": ["orderbook.50.${market}"]}`**
(space after `"args":`). **NiFi sends no ping.** On `disconnected`: rows → `pending-subscribe`.
`snapshot_request` → InvokeHTTP **through the proxy** `GET https://api.bybit.com/v5/market/orderbook?category=spot&symbol=${pair}&limit=50`
→ Jolt `default` action/pair → ex6-raw. WS direct (DNS alias).

**Real bybit (probed from a dev Mac):**
- ONE ack per subscribe message `{"success":true,"ret_msg":"subscribe","conn_id":"<20 a-z0-9>-<5>","op":"subscribe"}`
  (`req_id` echoed before `op`), then the snapshot at once. **Repeat subscribe → no reply at all.**
  Unsubscribe (even not subscribed) → `{"success":true,"ret_msg":"unsubscribe",…}`.
- `>10 args` → `"ret_msg":"args size \u003e10"` (Go-style escaping); `[]` → `"args required!"`;
  `orderbook.77.X` → `"Invalid topic :[orderbook.77.BTCUSDT]"`; unknown symbol → `"Invalid symbol :[…]"`
  (not copied); not-JSON incl. `ping` → `{"type":"error","ret_code":"400","ret_msg":"Invalid JSON!"}`;
  `{"op":"ping"}` → `{"success":true,"ret_msg":"pong","conn_id":…,"op":"ping"}`; `{"op":"login"}` → no reply.
- ~40 frames/s on BTCUSDT; `u` +1 exactly; `seq` +4…30 (shared across topics); `ts` = `cts` + 1…8 ms;
  delta ≈ 4.6 bids + 6 asks, one side often `[]`, with deletes (qty `"0"`) and new levels.
- REST: `{"retCode":0,"retMsg":"OK","result":{"s","a","b","ts","u","seq","cts"},"retExtInfo":{},"time":…}`;
  default limit 1; errors are HTTP 200 + `retCode` 10001 + `"result":{}`: `Illegal category`,
  `Parameter symbol [string] missing!`, `Not supported symbols`. `result.u` is another stream's
  counter (smaller); `result.seq` is the same counter as the WS `seq`.
- **No idle cut seen:** an 11-minute connection with no ping (data flowing) was NOT cut.
  So no Idle rule for bybit. (Not tested: a quiet market with no data and no ping.)

### ompfinex (7) — read from the flow + measured live 2026-10-21

**NiFi side (group `OMPFINEX`):** Centrifugo as NOBITEX (connect `{"id":1,"connect":{}}`, ≤10-byte
frames = ping → `{}`). Subscribe **`{"id":2,"${status:replace('pending-', '')}":{"channel":"public-market:r-depth-${market}","flag":1}}`**
(id 2, extra `flag`). `snapshot_request` → InvokeHTTP `GET https://api.ompfinex.com/v1/market/${pair}/depth?limit=50`
(no proxy) → Jolt `default` action/pair → ex7-raw. Market ids numeric (`14` = BTC/USDT).

**Real ompfinex (probed from a dev Mac):**
- connect reply `"version":"6.9.0 OSS","ping":6` — **ping every 6 s, no pong → 3012 after 4 s**.
- subscribe ack is **plain `{"id":2,"subscribe":{}}`** (no recoverable/epoch/offset); repeat → 105;
  `orderbook:14` → 102; empty market and unknown market (`99999`) → plain ack, no data.
- WS = **deltas only**: `{"u":7745591,"U":7745590,"a":[…],"b":[]}` (u first), U = previous u,
  u − U 1…3, deletes `"0"`; Centrifugo offset +1 per push; market 14 ~1 push / 2 s.
- REST 200 (`Content-Type: application/json`, no charset): `{"status":"OK","data":{"lastUpdateId":N,"time":<µs>,"bids":[…],"asks":[…]}}`,
  **25 levels by default**, 50 with `limit=50`; bids desc, asks asc. Unknown market → 400
  `{"status":"BAD_REQUEST","message":"Invalid market."}` (not copied).

### lbank (9) — read from the flow + measured live 2026-10-21

**NiFi side (group `LBANK`):** WS AND REST **through the proxy**. Subscribe
**`{"symbol": "${market}","pair": "${market}","action": "${status:replace('pending-', '')}","subscribe": "fdepth","depth": "50","limit": "50","clientType": ""}`**.
**Ping path = every frame of ≤ 60 bytes**: ReplaceText regex `"ping"\s*:\s*"([^"]+)"\s*,\s*"action"\s*:\s*"ping"`
→ `"pong":"$1","action":"pong"` → PutWebSocket. `snapshot_request` → InvokeHTTP (proxy)
`GET https://api.lbkex.com/v2/depth.do?symbol=${pair}&size=50` → Jolt action/pair → ex9-raw.
⚠ The LBANK ConnectWebSocket's client id is `#{bitget.web_socket.client_id}` (BITGET's) — each
group has its own JettyWebSocketClient, so probably harmless; reported to the user, not changed.

**Real lbank (probed from a dev Mac):**
- ack is **plain text** `<40 chars [A-Za-z0-9-]>_fdepth_btc_usdt_50` (59 bytes; same on a repeat),
  then the book at once, then ~2 frames/s. **59 ≤ 60 → NiFi's ping path echoes the ack back**,
  and lbank answers `{"SERVER":"V3","message":"message","status":"error","TS":…}` (→ Kafka, dropped).
- unsubscribe → no reply; `"subscribe":"foo"` → no reply; no `depth` → `"Missing parameter ['depth']"`;
  unknown pair → `"Invalid order pairs:[nope_usdt]"` (not copied); not-JSON → `"message"`.
- server ping `{"ping":"<ms>","action":"ping"}` every 60 s, first at 0.5/3.7/6.8/19.9 s (server-wide
  clock); correct pong (even twice) → no reply; wrong id → `"[<id>] is not a ping"`; client ping
  `{"action":"ping","ping":X}` → `{"action":"pong","pong":X}`.
- **No pong: closed 1001 "server-heartbeat-timeout" when the 7th ping was due** (6 unanswered, 379.9 s).
- frame `{"depth":{"asks","bids"},"SERVER":"V3","count":50,"limit":50,"type":"fdepth","pair":"btc_usdt","TS":"2026-10-22T04:16:24.032"}`,
  50/50, asks asc / bids desc, prices padded to 2 decimals, quantities 5 decimals.
- **⚠ `TS` is China time (UTC+8)** — UTC 20:16:00 read `04:16:24` next day. lpa-core's LBankParser
  reads TS as UTC (user decision 2026-08-26) → ex9 event times 8 h in the FUTURE. Reported to
  the user as an lpa-core finding; the simulator sends UTC+8 like real lbank.
- REST: `{"result":"true","msg":"Success","data":{"asks","bids","timestamp"},"error_code":0,"ts":…}`
  (`application/json;charset=UTF-8`, prices trimmed); **no `size` (or no/unknown symbol) → no
  `data` key at all**. LBankParser drops the REST body anyway (no depth/TS).
