---
name: conversation
description: Two HARD RULES on how to talk to the user — (2026-09-16) in Persian, never start a sentence, list item, heading or table cell with a non-Persian word, or the line's direction flips and it becomes unreadable; (2026-09-21) English is not the user's first language, so write every English reply in very simple words, short sentences, no idioms or fancy phrasing.
metadata:
    type: feedback
---

# Talking to the people who work on this

**In a Persian conversation, no sentence starts with a non-Persian word.**
Not an English word, a code identifier, a unit like `bps`, a file link or
backticked code. The same holds for the first word of every list item, heading
and table cell.

## Why

A line's direction is set by its first strong character. Open it with an English
word and the whole line is laid out left to right, so the Persian words after it
come out in the wrong order and the sentence cannot be read.

## How to apply

When the English term has to come early, rephrase so a Persian word leads:

| Not | But |
|---|---|
| `market` یک ستون است | ستون `market` … |
| `STALENESS_MS` جلوی اجرا را می‌گیرد | مقدار `STALENESS_MS ` … |
| bps مخفف basis points است | واحد bps مخفف … |

English inside a sentence is fine; only the opening word matters. Code blocks
are exempt, since they are a block of their own.

---

# Plain English only

**HARD RULE, user 2026-09-21: English is not the user's first language. Write
every English reply in very simple words.**

## Why

Hard words, idioms and set phrases (collocations) slow the user down or hide the
meaning. A reply that is correct but hard to read is a reply that did not work.

## How to apply

- Short sentences. One idea in each sentence.
- Common, everyday words. If a short word says it, use the short word.
- No idioms, no figures of speech, no clever phrasing ("blast radius",
  "load-bearing", "out of the box", "under the hood", "bite us later").
- When a technical term is needed (Kafka, topic, checkpoint, Flink job), keep the
  term and add a few plain words that say what it means.
- Lists and small steps beat long paragraphs.
- Say what happened, what you changed, what the user should do next.

This is about the words, not the content: keep the same care, tests and detail.
Only the wording gets simpler. Persian replies keep the rule above too.
