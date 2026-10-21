---
name: snapshot-requests
description: How lpa-core's pipeline asks NiFi for a fresh book (control-plane snapshot_request) and which exchanges can ever ask — what the simulator must answer and when
metadata:
  type: project
---

# Snapshot requests (from lpa-core, as of 2026-10-08)

When lpa-core's type validator (a Flink job) sees a sequence gap, it empties that book and
writes a `snapshot_request` to the Kafka topic `control-plane`. NiFi reads it (ConsumeKafka
with an Avro reader, by schema id), looks up the market in Postgres (ExecuteSQLRecord), keeps
it only if the market's status is `subscribe`, and sends it into that exchange's group. The
group then fetches a fresh book — by a REST call for most delta feeds — and publishes it.

**Which exchanges can ever ask:** only **bybit (6), ompfinex (7), okx (8)** — the delta feeds.
nobitex/bitpin stopped on 2026-09-02 (both feeds are full snapshots now), bitget on
2026-09-07; wallex, ramzinex and lbank never could.

**What this means for the simulator:**
- Its sequence chain never breaks by itself, so a request means NiFi or Kafka lost or
  reordered a frame — that is a test result, worth seeing in the report.
- The REST endpoint that answers must be the SIMULATOR's, or real prices enter a fake book.
  How NiFi reaches each REST host (direct or through a proxy) comes from the NiFi exports —
  see [[exchange-protocols]].
- In kafka mode nobody answers (no NiFi). See [[origin]].

The full story (why okx's REST body was once dropped, why a resubscribe is the better answer
for okx) is in lpa-core `memory/project_control_plane.md` and `memory/project_pair_extractor.md`.
