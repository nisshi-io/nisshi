---
paths:
  - "**/*.rs"
  - "**/*.sh"
  - "**/justfile"
---

## Names

A reader should understand what a name holds or does without reading its definition. Choose the
name that a new contributor understands on first reading, even if it is longer. A long name costs
a few characters, and an unclear name makes every reader look up its definition.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Name what the value means, not its type or its literal value.** From `TIMEOUT`, a reader
  learns what kind of value it is, but not what it limits. From `CONNECT_TIMEOUT`, a reader learns
  that it limits a connection attempt.
- **Name a function for its result or its effect.** A function that returns something is named
  for what it returns (`pending_requests`, `latest_offset`). A function that changes something
  starts with a verb (`create_topic`, `remove_member`). Avoid a bare noun such as `topic` or
  `request` for a function that creates or sends something.
- **Name a test for the behaviour it requires.** `expired_session_removes_member` states the rule
  that the test checks. `test_session` or `session_case_2` does not.
- **Spell words out.** Write `partition`, `message`, `configuration`, not `p`, `msg`, `cfg`. Terms
  and abbreviations that the Kafka documentation uses are fine: `acks`, `isr`, `api`.
- **Avoid generic words.** `data`, `info`, `result`, `value`, `item`, `helper`, `tmp`, `first`,
  `each` say nothing about the content. Add what the thing is: `first_attempt`,
  `records_per_batch`.
- **Name a type for what it is.** A type that only holds data is named for that data
  (`PartitionOffsets` holds a partition's offsets). A type with behaviour is named so the
  behaviour shows (`TemporaryDirectory` deletes itself when it is dropped).
- **Don't wrap a type in a new type to carry extra values.** If a function returns a
  `Connection` and some data about it, return the `Connection` and a type named for the data,
  such as `(Connection, Handshake)`. A name like `ConnectionWithHandshake` reads as a kind of
  `Connection`, and hides that it bundles separate values.
- **Make a boolean read as a statement.** `is_empty`, `port_is_free`, `include_defaults`.
- **Use one name for one thing.** If the code already has a name for a concept, use that name
  everywhere, and do not invent a synonym. Do not give two different things the same name.
- **A short name is fine only in a short scope.** A closure parameter or a loop index that is used
  on the next line can be `n` or `line`. A value that lives for more than a few lines needs a full
  name.

## The Test

Read the name alone, without its type and without the code around it. If you need to read the
definition to understand what it is, rename it.
