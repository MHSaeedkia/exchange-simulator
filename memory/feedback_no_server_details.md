---
name: no-server-details-in-memory
description: RULE (user, 2026-10-06) — memory/ and todo.md must never describe development servers or what was done on them; record only the general findings
metadata:
  type: feedback
---

**Never write anything about development servers into `memory/` or `todo.md`** — no host names,
IPs, SSH aliases, accounts, remote paths, run directories, the state a machine was left in, or an
account of what was run or what happened there. That includes test runs: "it passed" is fine,
"it passed on X, in dir Y" is not.

**What IS welcome:** an issue found while working there, written so it applies anywhere — the
symptom, the cause and the fix, with no reference to where it was seen (e.g. "a `sudo mvn` build
leaves root-owned `target/` dirs that break the e2e harness's `mvn clean`").

**Why:** the user's explicit instruction ("mentioning anything about development servers is
forbidden"), given after a session had recorded a server's address, layout and run history here.

**How to apply:** before saving, strip every location detail from the note; if nothing general is
left, save nothing. Report server-specific facts to the user in the conversation instead.
Pre-existing notes written before this rule still carry such details — rewriting those is the
user's call, not something to do unasked. Related: [[e2e-harness]].
