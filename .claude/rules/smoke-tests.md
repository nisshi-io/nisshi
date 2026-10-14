---
paths:
  - "nisshi-smoke-test/**"
---

## What a smoke test checks

The smoke suite checks that the real Kafka CLI tools work against Nisshi as a user runs them.
Nisshi develops the broker, not the tools, so a test asserts what the broker does, and never looks
for a bug in a tool. A value that a tool shows from the broker's answer is a fact about the broker:
the broker must send the tool what the tool needs to show the value correctly. A value that the
tool computes or formats itself is a fact about the tool. When the tool only changes the broker's
state, a test reads that state with `BrokerStateReader`. A tool's exit code and printed text are all
that the user sees, so a smoke test reads that text. The tools write the text for people, though. A
test that reads more of it than it needs breaks when a Kafka release changes the wording, and it
checks the tool instead of the broker.

## Rules

- **Check only the facts that the test's doc comment states.** Check the exit code, and read from
  the output only the values that the test is about. Leave the rest of the output alone: an empty
  stdout or a whole printed line is a fact about the tool, not the broker.
- **Check what the broker controls, not what the client chooses.** The Java client's partitioner
  decides which partition a key goes to, and the tool decides how to format a line. Check a fact
  that holds whatever the client chose, such as the total of the partitions' offsets instead of
  each partition's offset. The broker numbers the records within each partition, so check that
  numbering: each partition's offsets start at 0 and have no gap or repeat.
- **Test the broker's logic in `nisshi-broker/tests/it`.** A detailed check of broker behaviour,
  such as how it stores compressed batches, belongs in the integration tests, which send typed
  requests and check typed responses. Keep a detailed check in the smoke suite only if no other
  test can reach that behaviour, such as a process restart, several producers at once, or a topic
  that is deleted and created again.
- **Make a reader check the layout that it depends on.** A reader that splits a table at
  whitespace finds each column by its position. It must compare the table's heading with the
  heading it expects, and panic with the output if they differ. A new Kafka release then fails
  with a clear message instead of a wrong value. The reader returns `None` only if the layout
  matches and the value is not in the table.
- **Name every string that a reader or a test matches in a tool's output.** Put it in a constant
  at the top of that tool's module in `src/kafka_cli/`, so the module shows in one place what it
  expects the tool to print. Put the names of the Kafka exceptions that the tools print in
  `src/kafka_cli.rs`. Write a command-line argument as a literal where the command is built,
  because the harness writes it and does not read it.
- **Give each command that has a reader its own `Output` type.** A tool's method whose output a
  reader reads returns `Output<Marker>`, where `Marker` is an empty enum named for the command,
  such as `DescribeTopic` for `KafkaTopics::describe`. The reader is a method of
  `impl Output<Marker>`. The compiler then rejects a reader called on another command's output. A
  method whose output no reader reads returns `Output`.
- **Make a helper that panics report the test's line.** Put `#[track_caller]` on each helper, in
  `src/` or in a test file, that panics when the broker doesn't do what the test expects, and on
  each helper that calls it on the way from the test. nextest's first line for a failed test,
  `panicked at <file>:<line>`, then names the line in the test that failed. Without the attribute,
  every test that fails in one helper shows that helper's line, and the reader has to find the test
  among the 30 to 60 frames of the backtrace. The attribute passes the location only through an
  unbroken chain of tracked functions, and never through a closure, so a function panics in its
  own body, with `match` or `let ... else`, and not in `unwrap_or_else(|err| panic!(..))`. Leave the
  attribute off a panic that shows a bug in the harness itself, because the attribute then points
  away from the bug.
- **Make each panic message say what failed.** Each panic in one helper reports the same line in
  the test, so the message must tell them apart. Show the command, its exit code and what it
  printed, which the `Display` form of an `Output` holds, or the value that the test expected and
  the value that the broker gave.
