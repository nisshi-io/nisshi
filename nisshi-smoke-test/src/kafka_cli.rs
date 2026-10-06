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

//! Runs the Apache Kafka command-line tools against a broker.
//!
//! The tools (`kafka-topics.sh`, the console producer and consumer, and the others in
//! `/opt/kafka/bin`) run inside a container started from a Kafka image. The container uses the host
//! network, so the tools reach the broker at the address the broker advertises. Each call returns
//! the tool's exit code and output.
//!
//! Each call has two time limits. GNU `timeout` stops the tool inside the container, because
//! killing `docker exec` would leave the tool running there. [`timed_command::run`] kills a
//! `docker exec` that still hasn't returned [`GRACE`] later.

use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::{env, timed_command};

mod commands;

/// How long a tool a test runs has to finish.
const TOOL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long one readiness probe in [`KafkaCli::wait_until_ready`] has to finish.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// How much longer than a tool's own timeout the harness waits for
/// `docker exec` to return.
const GRACE: Duration = Duration::from_secs(15);
/// How long `timeout` waits after SIGTERM before it sends SIGKILL.
const KILL_AFTER: Duration = Duration::from_secs(5);

// The harness must wait longer than `timeout` takes to send SIGKILL, or it
// kills `docker exec` before `timeout` can report why the tool stopped.
const _: () = assert!(GRACE.as_secs() > KILL_AFTER.as_secs());

/// The exit status of GNU `timeout` when it stops the tool with SIGTERM.
const TIMEOUT_TERM_EXIT: i32 = 124;
/// The exit status of GNU `timeout` when it sends SIGKILL, because the tool
/// was still running [`KILL_AFTER`] after SIGTERM. `timeout` reports a signal
/// as a shell does, as 128 plus the signal number, and SIGKILL is 9.
const TIMEOUT_KILL_EXIT: i32 = 128 + 9;

/// The Kafka CLI tools, run with `docker exec` in a container on the host
/// network, so they reach a broker at the address it advertises.
#[derive(Debug)]
pub struct KafkaCli {
    container: String,
    bootstrap: String,
}

impl KafkaCli {
    /// Tools for the broker at `bootstrap`, in the `NISSHI_SMOKE_KAFKA`
    /// container that `just smoke` starts for the run.
    pub fn new(bootstrap: &str) -> Self {
        let container = env("NISSHI_SMOKE_KAFKA")
            .expect("NISSHI_SMOKE_KAFKA is not set: run the suite with `just smoke <engine>`");

        Self {
            container,
            bootstrap: bootstrap.to_owned(),
        }
    }

    /// Runs `/opt/kafka/bin/<tool>.sh <args> --bootstrap-server <broker>`. The address comes last,
    /// after a subcommand such as `kafka-cluster.sh cluster-id`, which takes it as its own option.
    pub fn run(&self, tool: &str, args: &[&str]) -> Output {
        self.exec(tool, args, None)
    }

    /// Like [`KafkaCli::run`], with `input` on the tool's stdin.
    pub fn run_with_input(&self, tool: &str, args: &[&str], input: &str) -> Output {
        self.exec(tool, args, Some(input))
    }

    /// Waits until the broker answers an `ApiVersions` request, or panics
    /// after `timeout`, or `GRACE` later if `docker exec` itself hangs.
    /// `alive` is checked between attempts, so a broker that has already
    /// exited fails fast with its reason.
    pub fn wait_until_ready(
        &self,
        timeout: Duration,
        mut alive: impl FnMut() -> Result<(), String>,
    ) {
        let deadline = Instant::now() + timeout;

        loop {
            if let Err(reason) = alive() {
                panic!(
                    "broker at {} stopped while starting: {reason}",
                    self.bootstrap
                );
            }

            // Each attempt gets at most the time left before the deadline, and at
            // least a second, because `timeout 0s` means no time limit.
            let left = deadline.saturating_duration_since(Instant::now());
            let limit = ATTEMPT_TIMEOUT.min(left).max(Duration::from_secs(1));

            let attempt = timed_command::run(
                &mut self.command("kafka-broker-api-versions", &[], false, limit),
                None,
                limit + GRACE,
            );

            match attempt {
                Ok(output) if output.code == Some(0) => return,

                attempt if Instant::now() >= deadline => {
                    panic!("broker at {} did not answer: {attempt:?}", self.bootstrap)
                }

                _ => thread::sleep(Duration::from_millis(500)),
            }
        }
    }

    fn exec(&self, tool: &str, args: &[&str], input: Option<&str>) -> Output {
        let mut command = self.command(tool, args, input.is_some(), TOOL_TIMEOUT);
        let line = format!("{tool} {}", args.join(" "));

        let finished = timed_command::run(&mut command, input, TOOL_TIMEOUT + GRACE)
            .unwrap_or_else(|err| panic!("{err}"));

        let timed_out = match finished.code {
            Some(TIMEOUT_TERM_EXIT) => Some(String::new()),
            Some(TIMEOUT_KILL_EXIT) => Some(format!(", nor within {KILL_AFTER:?} of SIGTERM")),
            _ => None,
        };

        if let Some(detail) = timed_out {
            panic!(
                "`{line}` did not finish within {:?}{detail}\nstdout:\n{}\nstderr:\n{}",
                TOOL_TIMEOUT, finished.stdout, finished.stderr
            );
        }

        Output {
            command: line,
            code: finished.code,
            stdout: finished.stdout,
            stderr: finished.stderr,
        }
    }

    fn command(&self, tool: &str, args: &[&str], interactive: bool, timeout: Duration) -> Command {
        let mut command = Command::new("docker");
        _ = command.arg("exec");

        if interactive {
            _ = command.arg("--interactive");
        }

        // The tool runs under `timeout` in the container, because killing the
        // `docker exec` client leaves the tool running there, still holding
        // its group membership on the broker.
        //
        // The Kafka image must provide GNU coreutils' `timeout`, because
        // BusyBox's returns the tool's own status instead of the exit statuses
        // that `exec` checks for.
        _ = command
            .arg(&self.container)
            .arg("timeout")
            .arg(format!("--kill-after={}s", KILL_AFTER.as_secs()))
            .arg(format!("{}s", timeout.as_secs()))
            .arg(format!("/opt/kafka/bin/{tool}.sh"))
            .args(args)
            .args(["--bootstrap-server", &self.bootstrap]);

        command
    }
}

/// What a Kafka CLI tool printed, and how it exited.
#[derive(Debug)]
pub struct Output {
    pub command: String,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    /// Asserts the tool exited with `code`, showing its output if not.
    #[track_caller]
    pub fn exited(&self, code: i32) -> &Self {
        assert_eq!(
            self.code,
            Some(code),
            "`{}` exited with {:?}\nstdout:\n{}\nstderr:\n{}",
            self.command,
            self.code,
            self.stdout,
            self.stderr
        );
        self
    }

    /// Asserts the tool exited with 0.
    #[track_caller]
    pub fn succeeded(&self) -> &Self {
        self.exited(0)
    }

    pub fn lines(&self) -> Vec<&str> {
        self.stdout.lines().collect()
    }
}
