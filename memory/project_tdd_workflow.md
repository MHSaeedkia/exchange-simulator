---
name: tdd-workflow
description: The team works test-first; tests state the contract, not the current code (copied from lpa-core, Flink-specific parts left out)
metadata:
  type: feedback
---

## Practice: test-first for all development

The team develops test-first (red → green → refactor) for **new** code: write a failing test
that states the desired behaviour, make it pass with the minimum code, then refactor.

**Existing code gets coverage written against the documented contract, not the
implementation.** Each test asserts what the code *should* do. If the code does something
else the test fails — that is a found bug to raise, **not** a reason to weaken the test.
Do not write pure characterization tests that just freeze current behaviour.

Rule of thumb: if the only possible unit assertion is "the builder returned non-null", it is an
integration test, not a unit test — don't write the worthless non-null test.

**Why:** a spec-based test in lpa-core found a real off-by-one in the sequence rule that a
test written from the code would have frozen in place.

**How to apply here:** each exchange's protocol is a contract (the real exchange's frames, as
the lpa-core parsers read them). Test the frames against that contract — e.g. against the
samples in `docs/sample-raw-data.md` — not against whatever the encoder happens to produce.
