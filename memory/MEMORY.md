# Memory Index

- [Conversation (HARD RULES)](conversation.md) — in Persian never open a line with a non-Persian word; English replies in very simple words, short sentences, no idioms
- [No server details](feedback_no_server_details.md) — RULE: never write development servers (hosts, IPs, paths, what was run there) into memory or todo.md; only general findings
- [Scope discipline](project_scope_discipline.md) — HARD RULE: do only what was asked; confirm any extra work first
- [TDD workflow](project_tdd_workflow.md) — test-first; tests state the contract (the real exchange's frames), not the current code
- [Project scope](project_scope.md) — **read first**: all 9 exchanges, real host names via DNS + own CA, stopped = silent, kafka mode kept, step by step from the NiFi processor-group exports (read → memory → ask → build), raw exports not committed
- [Origin](project_origin.md) — came from lpa-core's okx-only `load-generator` (branch `feat/load-generator`, kept there): the design calls (one goroutine per exchange sends everything, sending blocks and counts `missed`, sent rate per ACTIVE second) and the measured runs (0 rejects at up to 20k/s)
- [Exchange protocols](project_exchange_protocols.md) — fact sheet for the 9 exchanges (feed, endpoint, market key, sequence, frames, what NiFi adds); gaps marked NOT KNOWN until each export arrives
- [Snapshot requests](project_snapshot_requests.md) — when lpa-core asks NiFi for a fresh book; only bybit, ompfinex, okx can ask; the answering REST must be the simulator's
- [Redirect](project_redirect.md) — docker network aliases (not extra_hosts) + own CA; verified with a test client only; OPEN: NiFi truststore, okx REST proxy, the alias check against NiFi
- [NiFi Wallex websocket](project_nifi_wallex_ws.md) — (from lpa-core) Wallex socket facts: 51 subscribes per connection, 30-min cut, server PING; never Terminate a ConnectWebSocket
- [NiFi hot path](project_nifi_hot_path.md) — (from lpa-core) how each NiFi exchange group publishes (ReplaceText id/simulation, LookupAttribute, Run Duration) and its silent-failure traps
- [Build log](project_build_log.md) — what each step built, decisions made while building, and what was verified
