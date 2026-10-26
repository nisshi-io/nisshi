// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The first commands a new user runs, against the `nisshi` binary: `topic create`, `cat produce`
//! and `cat consume` on an in-memory broker.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a command may run before the test kills it. A test that waits without a limit hangs
/// a local run, and CI's nextest timeout kills the test with SIGKILL, which skips [`Broker`]'s
/// `Drop` and leaves the broker running.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// A `nisshi broker` on `memory://`, killed when it is dropped.
struct Broker {
    process: Child,
    url: String,
}

impl Broker {
    fn start() -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map(|address| address.port())
            .expect("free port");

        let url = format!("tcp://127.0.0.1:{port}");

        // The broker owns the process from here, so a failed start below kills it on the way out.
        let mut broker = Self {
            url: url.clone(),
            process: nisshi()
                .args([
                    "broker",
                    "--listener-url",
                    &url,
                    "--advertised-listener-url",
                    &url,
                    "--storage-engine",
                    "memory://nisshi/",
                    "--silent",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("broker starts"),
        };

        let started = Instant::now();

        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            if broker.process.try_wait().expect("broker status").is_some()
                || started.elapsed() > STARTUP_TIMEOUT
            {
                let output = broker.stop();
                panic!("broker did not listen on {url} within {STARTUP_TIMEOUT:?}: {output}");
            }

            thread::sleep(Duration::from_millis(50));
        }

        // Another process can take the port between the probe above and the broker's bind. The
        // broker then exits, and the connect succeeded against the other process.
        thread::sleep(Duration::from_millis(50));

        if broker.process.try_wait().expect("broker status").is_some() {
            panic!("broker exited after it started: {}", broker.stop());
        }

        broker
    }

    /// Stops the broker and returns how it exited and what it wrote, for a failure message.
    fn stop(&mut self) -> String {
        _ = self.process.kill();
        let status = self.process.wait();
        let stdout = self.process.stdout.take().map(read_to_end);
        let stderr = self.process.stderr.take().map(read_to_end);

        format!(
            "{status:?}, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&joined(stdout)),
            String::from_utf8_lossy(&joined(stderr))
        )
    }

    /// Starts `nisshi` with `args` and `--broker` set to this broker.
    fn spawn(&self, args: &[&str]) -> Child {
        nisshi()
            .args(args)
            .args(["--broker", &self.url])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("command starts")
    }

    /// Runs `nisshi` with `args` and `--broker` set to this broker, with `stdin` as its input.
    fn run(&self, args: &[&str], stdin: &str) -> Output {
        let mut process = self.spawn(args);
        write_stdin(&mut process, stdin);
        finish(process)
    }

    /// Runs `nisshi` as [`Broker::run`] does, with the read end of its stdout closed before it
    /// starts, as `nisshi ... | true` does.
    fn run_with_closed_stdout(&self, args: &[&str], stdin: &str) -> Output {
        let mut process = self.spawn(args);
        drop(process.stdout.take());
        write_stdin(&mut process, stdin);
        finish(process)
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        _ = self.process.kill();
        _ = self.process.wait();
    }
}

/// The `nisshi` binary, with an empty environment, run from a directory without a `.env`, so
/// that neither the caller's environment nor a developer's `.env` changes the broker or the
/// command output.
fn nisshi() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nisshi"));
    _ = command.env_clear().current_dir(std::env::temp_dir());
    command
}

/// Writes `stdin` to the process and closes its stdin.
fn write_stdin(process: &mut Child, stdin: &str) {
    process
        .stdin
        .take()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("stdin written");
}

/// Waits for `process` to exit and returns its output. Kills it after [`COMMAND_TIMEOUT`].
fn finish(mut process: Child) -> Output {
    // Threads drain stdout and stderr while the process runs, so that a full pipe can't stop it.
    let stdout = process.stdout.take().map(read_to_end);
    let stderr = process.stderr.take().map(read_to_end);
    let started = Instant::now();

    loop {
        if let Some(status) = process.try_wait().expect("command status") {
            return Output {
                status,
                stdout: joined(stdout),
                stderr: joined(stderr),
            };
        }

        if started.elapsed() > COMMAND_TIMEOUT {
            _ = process.kill();
            _ = process.wait();

            panic!(
                "command did not exit within {COMMAND_TIMEOUT:?}, stdout: {}, stderr: {}",
                String::from_utf8_lossy(&joined(stdout)),
                String::from_utf8_lossy(&joined(stderr))
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

/// Reads `pipe` to its end on a thread of its own.
fn read_to_end(mut pipe: impl Read + Send + 'static) -> JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = vec![];
        _ = pipe.read_to_end(&mut bytes);
        bytes
    })
}

fn joined(reader: Option<JoinHandle<Vec<u8>>>) -> Vec<u8> {
    reader
        .map(|reader| reader.join().expect("pipe reader"))
        .unwrap_or_default()
}

fn stdout(output: &Output) -> String {
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

#[test]
fn topic_create_and_cat_produce_confirm_success() {
    let broker = Broker::start();

    assert_eq!(
        "Created topic taxi with 3 partitions.\n",
        stdout(&broker.run(&["topic", "create", "taxi"], ""))
    );

    assert_eq!(
        "Produced 2 messages to taxi partition 0, offsets 0 to 1.\n",
        stdout(&broker.run(
            &["cat", "produce", "taxi"],
            "{\"value\": \"hello\"}\n{\"value\": \"world\"}\n"
        ))
    );

    assert_eq!(
        "Produced 1 message to taxi partition 0, offset 2.\n",
        stdout(&broker.run(&["cat", "produce", "taxi"], "{\"value\": \"again\"}\n"))
    );

    let consumed = stdout(&broker.run(&["cat", "consume", "taxi"], ""));

    for value in ["hello", "world", "again"] {
        assert!(consumed.contains(value), "{value} not in {consumed}");
    }
}

#[test]
fn silent_prints_nothing_on_success() {
    let broker = Broker::start();

    assert_eq!(
        "",
        stdout(&broker.run(&["topic", "create", "taxi", "--silent"], ""))
    );

    assert_eq!(
        "",
        stdout(&broker.run(
            &["cat", "produce", "taxi", "--silent"],
            "{\"value\": \"hello\"}\n"
        ))
    );
}

#[test]
fn failed_topic_create_prints_no_confirmation() {
    let broker = Broker::start();

    _ = stdout(&broker.run(&["topic", "create", "taxi"], ""));

    let duplicate = broker.run(&["topic", "create", "taxi"], "");

    assert_eq!(Some(1), duplicate.status.code(), "{duplicate:?}");
    assert!(
        !String::from_utf8_lossy(&duplicate.stdout).contains("Created topic"),
        "{duplicate:?}"
    );
}

#[test]
fn failed_cat_produce_prints_no_confirmation() {
    let broker = Broker::start();

    _ = stdout(&broker.run(&["topic", "create", "taxi", "--partitions", "3"], ""));

    let missing_partition = broker.run(
        &["cat", "produce", "taxi", "--partition", "7"],
        "{\"value\": \"hello\"}\n",
    );

    assert_eq!(
        Some(1),
        missing_partition.status.code(),
        "{missing_partition:?}"
    );
    assert!(
        !String::from_utf8_lossy(&missing_partition.stdout).contains("Produced"),
        "{missing_partition:?}"
    );
}

/// A command whose stdout reader has gone, as in `nisshi topic create taxi | true`, has still
/// done its work. It must exit 0, because a caller that retries a failed `cat produce` writes
/// the messages again.
#[test]
fn closed_stdout_does_not_fail_a_command_that_succeeded() {
    let broker = Broker::start();

    let created = broker.run_with_closed_stdout(&["topic", "create", "taxi"], "");
    assert!(created.status.success(), "{created:?}");

    let produced =
        broker.run_with_closed_stdout(&["cat", "produce", "taxi"], "{\"value\": \"hello\"}\n");
    assert!(produced.status.success(), "{produced:?}");

    assert_eq!(
        "[{\"key\":null,\"value\":\"hello\"}]\n",
        stdout(&broker.run(&["cat", "consume", "taxi"], ""))
    );
}
