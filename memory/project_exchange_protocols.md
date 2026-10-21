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
| 4 | ramzinex | WS Centrifugo, full snapshot both sides | NOT KNOWN | numeric `12` (channel `orderbook:12`) | `pub.offset`, jump 0 | none (processing time) |
| 5 | bitget | WS `books50`, snapshot only | NOT KNOWN | `BTCUSDT` (`arg.instId`) | `data[i].seq` (int), jump 0 | inner `ts` string ms |
| 6 | bybit | WS `orderbook.50.{sym}` snapshot+delta, REST `/v5/market/orderbook` | WS `wss://stream.bybit.com/v5/public/spot` *(measured directly, NiFi use inferred)* | `BTCUSDT` (`data.s` / `result.s`) | WS `data.u`, jump 1; snapshot with `u==1` = restart (null); REST null | `cts` |
| 7 | ompfinex | REST snapshot + Centrifugo WS delta (NO captures, shapes from the parser only) | NOT KNOWN | numeric `14` (channel `public-market:r-depth-14`) | REST `data.lastUpdateId`, jump 0; WS `u`, jump `u−U` | REST `data.time` µs ÷ 1000; WS processing time |
| 8 | okx | WS `books` snapshot+update, REST snapshot | WS `wss://ws.okx.com:8443/ws/v5/public`; REST `https://www.okx.com/api/v5/market/books` — both through the proxy | `BTC-USDT` (`arg.instId`) | `seqId`, jump `seqId−prevSeqId`; snapshot jump 0; REST null | `ts` string ms |
| 9 | lbank | WS `fdepth`, snapshot only | NOT KNOWN | `btc_usdt` (lower case, underscore) | none (null) | `TS` ISO without zone, read as UTC |

## Per exchange — details

- **nobitex (1)**: WS frame `{"push":{"channel":"public:orderbook-BTCUSDT","pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}`, levels `[str,str]`, 24/side. Built in step 2 — see § nobitex below.
- **bitpin (2)**: WS channel `orderbook:BTC_USDT`; `data` = `bids, asks, volume_ask, volume_bid, symbol, event:"market_data", price`, `event_time`; 50 levels. Built in step 3 — see § bitpin below.
- **wallex (3)**: first frame `{"sid":…,"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}`; server sends a control PING every 20 s, closes at 30 s without PONG; cuts EVERY connection at exactly 30 min. Frame `["BTCUSDT@buyDepth",[{"price":62525.04,"quantity":0.000451,"sum":…}]]` (numbers). 2 subscribes per market (buy + sell); no snapshot on subscribe. **Limit: 51 subscribe messages per connection for its whole life**, #52+ ignored silently. Subscribe text NOT KNOWN. Facts in [[project-nifi-wallex-ws]].
- **ramzinex (4)**: `data.buys`/`data.sells` 7-element numeric arrays `[price,qty,notional,false,null,int,epochMs]`, BOTH sides sorted descending. Subscribe NOT KNOWN.
- **bitget (5)**: `{"action":"snapshot","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"},"data":[{asks,bids,"ts":"…","seq":N,"pseq":0}],"ts":N}`. A snapshot answer must be a resubscribe (a REST body would be dropped). Subscribe NOT KNOWN.
- **bybit (6)**: `{"topic":"orderbook.50.BTCUSDT","ts":…,"type":"snapshot|delta","data":{"s","b","a","u","seq"},"cts":…}`; an unchanged side on a delta is `[]`. More than 10 args in one subscribe → `{"success":false,"ret_msg":"args size >10"}` and then NOTHING. REST body under `result`. Subscribe `{"op":"subscribe","args":["orderbook.50.BTCUSDT"]}` *(inferred)*.
- **ompfinex (7)**: REST `{"status":"OK","action":"snapshot","pair":"14","data":{"lastUpdateId":N,"time":µs,"bids","asks"}}`; WS `data.U`, `data.u`, `a`, `b`, qty `"0"` = delete. Subscribe NOT KNOWN.
- **okx (8)**: built in step 1 (from the okx-only version). Subscribe/unsubscribe `{"op":"subscribe|unsubscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}`; ack `{"event":"subscribe","arg":{…},"connId":…}`; errors 60012 / 60018; text `ping` → `pong`. Frame `{"arg":{…},"action":"snapshot|update","data":[{asks,bids,ts,checksum,prevSeqId,seqId}]}`, 4-element levels, 400 levels on a snapshot. REST `{"code":"0","msg":"","data":[{asks,bids,ts,seqId}]}`.
- **lbank (9)**: `{"depth":{asks,bids},"SERVER":"V3","count":200,"limit":50,"type":"fdepth","pair":"btc_usdt","TS":"2026-08-25T17:46:51.723"}`. Ping `{"action":"ping","ping":"…"}` and sub-ack `{"status":"success","pair":"btc_usdt"}` exist only as hand-made test fixtures. Subscribe NOT KNOWN.

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
