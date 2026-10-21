---
name: exchange-protocols
description: Per-exchange fact sheet for the 9 exchanges (feed type, endpoints, subscribe text, frame shape, market key, sequence, what NiFi adds, snapshot answer, limits) — filled in from each NiFi processor-group export; gaps marked
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
- **NOT KNOWN for any exchange yet:** how NiFi trusts TLS (SSLContextService or the JVM
  default), and the okx REST proxy setup.

| # | exchange | feed | known endpoint | market key (DB) | sequence | event time |
|---|---|---|---|---|---|---|
| 1 | nobitex | REST snapshot + WS Centrifugo, BOTH full snapshots | NOT KNOWN | `BTCUSDT` (WS channel `public:orderbook-BTCUSDT`) | WS `pub.offset`, jump 0; REST null | `lastUpdate` epoch ms (number) |
| 2 | bitpin | REST snapshot + WS Centrifugo, both full snapshots | NOT KNOWN | `BTC_USDT` (underscore, REST `pair` must match) | WS `pub.offset`, jump 0; REST null | WS ISO string, REST epoch ms number |
| 3 | wallex | WS only; each frame = full snapshot of ONE side | `wss://api.wallex.ir/ws` | `BTCUSDT` (`BTCUSDT@buyDepth` / `@sellDepth`) | none (null) | none (processing time) |
| 4 | ramzinex | WS Centrifugo, full snapshot both sides | NOT KNOWN | numeric `12` (channel `orderbook:12`) | `pub.offset`, jump 0 | none (processing time) |
| 5 | bitget | WS `books50`, snapshot only | NOT KNOWN | `BTCUSDT` (`arg.instId`) | `data[i].seq` (int), jump 0 | inner `ts` string ms |
| 6 | bybit | WS `orderbook.50.{sym}` snapshot+delta, REST `/v5/market/orderbook` | WS `wss://stream.bybit.com/v5/public/spot` *(measured directly, NiFi use inferred)* | `BTCUSDT` (`data.s` / `result.s`) | WS `data.u`, jump 1; snapshot with `u==1` = restart (null); REST null | `cts` |
| 7 | ompfinex | REST snapshot + Centrifugo WS delta (NO captures, shapes from the parser only) | NOT KNOWN | numeric `14` (channel `public-market:r-depth-14`) | REST `data.lastUpdateId`, jump 0; WS `u`, jump `u−U` | REST `data.time` µs ÷ 1000; WS processing time |
| 8 | okx | WS `books` snapshot+update, REST snapshot | WS `wss://ws.okx.com:8443/ws/v5/public`; REST path `/api/v5/market/books` *(inferred)* through a proxy | `BTC-USDT` (`arg.instId`) | `seqId`, jump `seqId−prevSeqId`; snapshot jump 0; REST null | `ts` string ms |
| 9 | lbank | WS `fdepth`, snapshot only | NOT KNOWN | `btc_usdt` (lower case, underscore) | none (null) | `TS` ISO without zone, read as UTC |

## Per exchange — details

- **nobitex (1)**: WS frame `{"push":{"channel":"public:orderbook-BTCUSDT","pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}`, levels `[str,str]`, ~24/side. Subscribe NOT KNOWN (Centrifugo `{"subscribe":{"channel":…}}` *(inferred)*).
- **bitpin (2)**: WS channel `orderbook:BTC_USDT`; `data` = `bids, asks, volume_ask, volume_bid, symbol, event:"market_data", price` (number), `event_time`; 50 levels. Subscribe NOT KNOWN.
- **wallex (3)**: first frame `{"sid":…,"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}`; server sends a control PING every 20 s, closes at 30 s without PONG; cuts EVERY connection at exactly 30 min. Frame `["BTCUSDT@buyDepth",[{"price":62525.04,"quantity":0.000451,"sum":…}]]` (numbers). 2 subscribes per market (buy + sell); no snapshot on subscribe. **Limit: 51 subscribe messages per connection for its whole life**, #52+ ignored silently. Subscribe text NOT KNOWN. Facts in [[project-nifi-wallex-ws]].
- **ramzinex (4)**: `data.buys`/`data.sells` 7-element numeric arrays `[price,qty,notional,false,null,int,epochMs]`, BOTH sides sorted descending. Subscribe NOT KNOWN.
- **bitget (5)**: `{"action":"snapshot","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"},"data":[{asks,bids,"ts":"…","seq":N,"pseq":0}],"ts":N}`. A snapshot answer must be a resubscribe (a REST body would be dropped). Subscribe NOT KNOWN.
- **bybit (6)**: `{"topic":"orderbook.50.BTCUSDT","ts":…,"type":"snapshot|delta","data":{"s","b","a","u","seq"},"cts":…}`; an unchanged side on a delta is `[]`. More than 10 args in one subscribe → `{"success":false,"ret_msg":"args size >10"}` and then NOTHING. REST body under `result`. Subscribe `{"op":"subscribe","args":["orderbook.50.BTCUSDT"]}` *(inferred)*.
- **ompfinex (7)**: REST `{"status":"OK","action":"snapshot","pair":"14","data":{"lastUpdateId":N,"time":µs,"bids","asks"}}`; WS `data.U`, `data.u`, `a`, `b`, qty `"0"` = delete. Subscribe NOT KNOWN.
- **okx (8)**: built in step 1 (from the okx-only version). Subscribe/unsubscribe `{"op":"subscribe|unsubscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}`; ack `{"event":"subscribe","arg":{…},"connId":…}`; errors 60012 / 60018; text `ping` → `pong`. Frame `{"arg":{…},"action":"snapshot|update","data":[{asks,bids,ts,checksum,prevSeqId,seqId}]}`, 4-element levels, 400 levels on a snapshot. REST `{"code":"0","msg":"","data":[{asks,bids,ts,seqId}]}`.
- **lbank (9)**: `{"depth":{asks,bids},"SERVER":"V3","count":200,"limit":50,"type":"fdepth","pair":"btc_usdt","TS":"2026-08-25T17:46:51.723"}`. Ping `{"action":"ping","ping":"…"}` and sub-ack `{"status":"success","pair":"btc_usdt"}` exist only as hand-made test fixtures. Subscribe NOT KNOWN.

## Filled in from NiFi exports

(One section per exchange, added as each export is read.)
