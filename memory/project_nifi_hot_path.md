---
name: project-nifi-hot-path
description: Why the NiFi per-exchange ingest path stalled at ~100 msg/s and the 2026-08-31 redesign that removed the per-message Redis call and the Jolt recompile
metadata:
  type: project
---

**Symptom (2026-08-31, reported on the BYBIT group but true of all 9):** at ~5000 msg/s the websocket path collapsed to ~100 in/out and **stayed** collapsed. It only recovered by emptying the queue or stop/starting the downstream processor — i.e. a stuck state, not merely slowness. The queue built up behind `FetchDistributedMapCache`, which is why that processor looked guilty.

**Two real causes, both on the hot path, both since removed:**

1. **`JoltTransformJSON` with Expression Language in its spec.** The spec was `{"id":"${uuid}","simulation":0}` with **Transform Cache Size 1**. NiFi resolves the EL first and caches the *compiled* transform keyed by the resolved spec text — and `${uuid}` differs per FlowFile, so the cache key is unbounded and **every single message paid a full Jolt chain parse+compile**. This was the actual bottleneck; the Fetch queue was just back pressure from it. Non-obvious because the spec *looks* static.
2. **`FetchDistributedMapCache` doing a Redis round-trip per message for a constant** (`bybit` → `exchange_id`). One network hop × 5000/s × 9 groups.

**Decision — the replacement (applied to ALL 9 exchange groups 2026-08-31):**

- Redis lookup → **`SimpleDatabaseLookupService` (`nifi-lookup-services-nar`) + `LookupAttribute`**. Reads `exchanges` (key col `name`, value col `id`) through the existing `DBCPConnectionPool`, held in an in-memory cache with `Cache Expiration` as the refresh knob. **The user explicitly chose dynamic-from-DB over a static parameter**, and postgres is a better source of truth for `exchanges.id` than a Redis key some other flow has to remember to populate. Cost per message drops from a network hop to a hashmap get. **DistributedMapCache has no cached variant** — `FetchDistributedMapCache` and `DistributedMapCacheLookupService` are both one Redis call *per FlowFile*, which is the whole problem; a cache-backed lookup service is the only way to be dynamic AND free.
- Jolt → **`ReplaceText`** in Regex Replace / Entire text, search `^\s*\{`, replacement `{"id":"${uuid}","simulation":0,`. The `default` operation was only prepending two keys to a JSON object; no JSON parsing is needed for that.
- **Run Duration `25ms`** on the two new processors. It batches N `onTrigger` calls into ONE session commit (the commit — FlowFile repo + provenance disk writes — is the cost, not the CPU), so ~5000 commits/s becomes ~40. Only for `@SupportsBatching` processors; **never on `PutSQL`/`PutWebSocket`/`Put*`** — a failed batch rolls back and replays, which duplicates external writes. Telling detail: every other hot-path processor in the original export was already at `25ms`; the only two at `0` were the two that turned out to be the bottleneck.

**⚠ Two silent-failure traps this design introduces:**

- `LookupAttribute`'s **`unmatched` must NOT flow downstream**. The attribute feeds `kafak_topic_raw_data` = `ex${exchange_id}-raw` (its only consumer — see [[kafka-topic-strategy]]); missing ⇒ publishes to `ex-raw`, a topic no Flink job reads. Route `unmatched`+`failure` to the same LogAttribute the old `not-found` went to.
- **A `ReplaceText` regex that does not match is not an error** — content passes through `success` unchanged, with no `id`. Job 1 then drops it as `dropped-no-id`, i.e. **100% data loss on that feed with nothing anywhere in NiFi showing an error** (see [[record-lineage]]). `^\s*\{` rather than `^\{` for leading whitespace, and `dropped-no-id` is the only real proof the change worked.
- Minor: Jolt's `default` added the keys only if absent; a prepend always adds them. Safe today (no bybit payload has a root `id`/`simulation`), but a future exchange field would give a duplicate key and last-wins parsers would take theirs.

**Rejected: `ReplaceTextWithMapping`.** Its map comes from a **local file** on the node (`Mapping File` + `Mapping File Refresh Interval`), not from a DB or Redis; it rewrites **content**, not attributes; and it scans the whole body per message. Wrong tool on all three counts.

**Correction worth keeping:** "run a `GenerateFlowFile` every 60 s to fetch the value into an attribute" does **not** work — attributes belong to one FlowFile and cannot reach the message FlowFiles. Only a parameter or a caching lookup service avoids the per-message hop.

**Still open / unverified:**

- No before/after throughput number was recorded, so the fix is applied but not measured here.
- **Redis connection pool sizing.** All Redis processors in all 9 groups share ONE `RedisDistributedMapCacheClientService` → one connection pool whose NiFi default is **Max Total 8, Block When Exhausted true, Max Wait 10 s**. The hot-path readers are gone, but `Wait`, the websocket-session `Put/FetchDistributedMapCache` and the conn-state keys still use it. If stalls recur, check that first.
- **Queue swapping was a hypothesis, never confirmed.** `nifi.queue.swap.threshold` defaults to 20000 while back pressure was already 10000, so it should not have been reached — *unless* `ConnectWebSocket` creates FlowFiles from the Jetty callback thread and bypasses back pressure entirely (unverified). The observable test: if the stuck queue sits at ~10000 there is no swapping; if it climbs to 50k+ there is.
- **NiFi has no thread-dump button in the UI** (Summary → System Diagnostics gives counts only). Use `docker exec <c> /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/d.txt` (no filename ⇒ goes to `logs/nifi-bootstrap.log`), take 3 dumps ~5 s apart, and read only the `Timer-Driven Process Thread` entries. ⚠ **Wrong for 2.11 (seen 2026-09-28):** `nifi.sh` has no `dump` command any more (usage lists `diagnostics`). Use `nifi.sh diagnostics --verbose /tmp/d.txt`, which I think includes the thread dump — not yet checked. See [[project-nifi-https]] for the container setup.

---

**2026-09-08 — the ordering question this file never asked.** The 2026-08-31 work fixed
THROUGHPUT on the bybit group; it did not consider ORDER. ex6/bybit is now dead-lettering
`sequence_gap` in job 2, and bybit's `u` was measured straight off the exchange socket as **+1 on
9213/9213 consecutive transitions** (20 symbols, one connection, 60 s) — so the exchange is
contiguous and the disorder is ours. Suspect the publish path: this repo's own
`docker-compose.yml` notes ~5-8 producer ids on `ex{id}-raw`, and a single partition orders
*appends*, not FlowFiles, once N threads publish concurrently. Check, in order: `PublishKafka`
Concurrent Tasks (must be 1 for an ordered feed), a `FirstInFirstOutPrioritizer` on every queue
on the path, and `max.in.flight.requests.per.connection` on the producer. Note this trades
throughput for order — the exact axis 2026-08-31 optimised the other way, so it needs a decision,
not a reflex. See the 2026-09-08 § of [[project-pair-extractor]].

---

**2026-09-09 — reading the BYBIT group export against the three 2026-09-08 suspects.**
A two-VM A/B test was run: VM1 a standalone Go websocket→Kafka producer, VM2 this NiFi group,
both on `BTCUSDT`, both watched by a `u`-contiguity script. VM1 clean for a long run, VM2 broke
within seconds. Verdict on the suspects, from the flow JSON:

- **Concurrent Tasks — NOT the cause.** The whole WS path is already single-threaded:
  `ConnectWebSocket`, `LookupAttribute`, `ReplaceText`, `UpdateAttribute`, the output port and
  `PublishKafka` are all `concurrentlySchedulableTaskCount: 1`. Suspect #1 is closed.
- **FIFO prioritizer — CONFIRMED MISSING.** Every connection in the group is `"prioritizers":[]`.
  Undefined order is documented NiFi behaviour, so this stays open, but note it only bites where
  something can actually reorder a single-threaded chain (penalized FlowFiles, or swapping).
- **`max.in.flight.requests.per.connection` — not exposed** by `Kafka3ConnectionService` and not
  in the flow. Moot *while* `Transactions Enabled: true`, because transactions force
  `enable.idempotence` and Kafka then keeps per-partition order. **Turning transactions off to fix
  the isolation mismatch below would remove that guarantee with no exposed knob to restore it** —
  do not treat the two changes as independent.

**The A/B test itself is confounded — fix before drawing conclusions.** `PublishKafka` has
`Transactions Enabled: true`; the Go producer does not use transactions. Both the seqwatch reader
(franz-go defaults to `ReadUncommitted`, verified in `config.go`) and job 1's `KafkaSource` (no
`isolation.level` set anywhere in `flink/`) read **uncommitted**. So on the NiFi VM the reader also
sees records from ABORTED transactions, and on the Go VM there is nothing to abort. This alone can
manufacture non-contiguous `u` with NiFi perfectly ordered. It also means job 2's `sequence_gap`
dead letters may be reading aborted writes.

**Loss is silent in three places**, each ending at a `LogAttribute` that auto-terminates `success`,
with no counter: `PublishKafka` `failure` (Failure Strategy = Route to Failure), the id-injection
`ReplaceText` `failure`, and `LookupAttribute` `failure`/`unmatched` (that last one intended, see
above). Because transactions batch, ONE `PublishKafka` failure drops **every** FlowFile in that
batch — loss arrives in bursts, which is what "many gaps in a couple of seconds" looks like.
`ack.wait.time` and `max.block.ms` are both **5 sec**, which makes those failures reachable.

**The sign of the gap tells you which fault it is**, and the script already logs it: `> 1` = loss
(the drop paths), `0` or negative = duplicate/reorder (aborted transactions read uncommitted, or
swapping). Read `gap_detected.log` before changing anything.

**Still unverified:** whether `ConnectWebSocket` really bypasses back pressure (the 2026-08-31 open
item). It matters more now: if it does, the queue can pass `nifi.queue.swap.threshold` (20000)
while back pressure is 10000, and swapping is the one mechanism that reorders a single-threaded
chain. Same observable as before — queue at ~10000 means no swap, 50k+ means swap.

**Unrelated leftover found while reading:** the REST-snapshot branch still runs the
`JoltTransformJSON` with `${pair}` in its spec at `Transform Cache Size 1` — the per-message
recompile the 2026-08-31 work removed from the WS path never got removed from this one
(`InvokeHTTP` beside it is at 4 concurrent tasks). Not the `u` gaps; same landmine.

**2026-09-09 (later) — MEASURED: it is REORDERING, not loss. Adjacent-pair swap.**
`gap_detected.log` from the NiFi VM, 5 entries: 4 of them are `difference: 2` with the missing `u`
arriving on the VERY NEXT line (…412, 414, 413…). The 5th is a slightly longer shuffle
(179, 181, 183). Nothing is missing and nothing is duplicated — neighbours change places. The user
also confirms **zero FlowFiles have ever reached the `LogAttribute` on `PublishKafka`'s `failure`**.

This closes most of the file above:

- **Loss is NOT happening.** The silent-drop paths, the 5-sec `max.block.ms`/`ack.wait.time`, and
  the transactional-abort-visible-to-read_uncommitted theory are all real hazards but **none of
  them is the ex6 gap**. Do not "fix" them in response to this symptom — raising those timeouts
  for a realtime feed trades data loss for stale data, and the user was right to push back on it.
  (`read_committed` on job 1 is still correct on its own merits; it is not this bug.)
- **Suspect #2 (no `FirstInFirstOutPrioritizer`) is the only one left standing**, and it is
  confirmed missing on all five connections of the WS→Kafka path.

**The mechanism, INFERRED and NOT YET VERIFIED:** `ConnectWebSocket` does not emit from a scheduled
processor thread — it creates one session per websocket message from the Jetty callback and commits
it asynchronously, so the order those commits land in the first queue need not be the arrival
order. Nothing downstream re-sorts, because a queue with no prioritizer has undefined order by
NiFi's own documentation. Note a single partition + a transactional (therefore idempotent) producer
+ 1 concurrent task everywhere means the producer CANNOT be the reorder point — which is what
pushed the search back upstream into the FlowFile path.

**Fix being tried:** `FirstInFirstOutPrioritizer` on all five connections. It sorts by FlowFile
creation time, so a swapped pair is put back in order before the next processor takes it. **Limit:
it only helps while both FlowFiles are in the queue together** (the `Run Duration 25ms` batching
makes that likely, not certain). If it does not hold, the next step is a `LogAttribute` on the
`uuid` immediately after `ConnectWebSocket`, compared against Kafka order, to prove whether the
swap happens at reception or later.

**2026-09-09 (resolved) — `OldestFlowFileFirstPrioritizer` on the five WS→Kafka connections FIXES
the ex6 reordering.** Confirmed by the user against the same seqwatch/`u`-contiguity test that had
been breaking within seconds.

**Take the naming trap with you — it is the whole lesson.** `FirstInFirstOutPrioritizer` is the
obvious-looking choice and it is the WRONG one here: it sorts by when a FlowFile reached *that
connection*, so it re-derives order at every hop and faithfully preserves a swap that happened
upstream. `OldestFlowFileFirstPrioritizer` sorts by **lineage start date** — when the FlowFile was
created, i.e. when the websocket message actually arrived — and that timestamp is carried unchanged
through every processor, so one setting is correct at all five hops. That the fix works is also
evidence for the mechanism inferred above: the pair is already swapped when it lands in the FIRST
queue (FlowFiles created in order on the Jetty thread, committed out of order), because sorting by
creation time repairs it while sorting by queue arrival would not have.

**Known limits of the fix, worth re-testing if gaps ever return:**
- Lineage start date is **milliseconds**. Two messages created in the same millisecond are a tie
  and their order is arbitrary again. Fine for one symbol at bybit's ~20 ms push rate; NOT
  obviously fine if a group ever carries many symbols on one connection at high rate.
- A prioritizer only sorts what is **in the queue at that moment**. It cannot help if the later
  message is pulled before the earlier one has arrived. The `Run Duration 25ms` batching is what
  keeps several FlowFiles in the queue together and gives the prioritizer something to sort — so
  the 2026-08-31 throughput change is quietly load-bearing for the ordering fix.

**Apply this to the other 8 exchange groups.** Nothing about the mechanism is bybit-specific: every
group has the same `ConnectWebSocket` → … → `PublishKafka` shape and the same empty prioritizer
lists. ex6 was simply the one with a `u` counter contiguous enough to expose it.
