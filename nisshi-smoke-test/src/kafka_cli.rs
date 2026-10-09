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
//! [`KafkaCli`] runs each tool that [`Tool`] lists. Each tool has a module with two kinds of methods. Methods on
//! [`KafkaCli`] build the tool's command-line arguments, so a test names an action, such as
//! creating a topic, and does not list flags. Methods on [`Output`] read what the tool printed. A
//! method returns an [`Output`] typed by its command, so a test can call only the readers for
//! that command's output.
//!
//! Each method's doc names the Kafka APIs that the command is for. A command sends other requests
//! too, such as `ApiVersions` and `Metadata` first, and which ones it sends can change between
//! Kafka versions.
//!
//! The tools in `/opt/kafka/bin` run inside a container started from a Kafka image. The container
//! uses the host network, so the tools reach the broker at the address the broker advertises. Each
//! call returns the tool's exit code and output.
//!
//! Every time limit in the harness is a watchdog for a test that has already failed. A passing test
//! must stop when it reaches the result it expects, such as a record count, and must not wait for
//! a limit to expire. Each limit can then be well above the slowest passing run.
//!
//! Each call has two time limits. GNU `timeout` stops the tool inside the container, because
//! killing `docker exec` would leave the tool running there. [`timed_command::run`] kills a
//! `docker exec` that still hasn't returned [`GRACE`] later.

use std::{
    fmt,
    marker::PhantomData,
    net::{TcpStream, ToSocketAddrs},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::{settings, timed_command, unique_name};

mod cluster;
mod configs;
mod console_consumer;
mod console_producer;
mod consumer_groups;
mod delete_records;
mod offsets;
mod topics;
mod verifiable_producer;

/// The name the tools print for the broker's `TOPIC_ALREADY_EXISTS` error.
pub const TOPIC_EXISTS_EXCEPTION: &str = "TopicExistsException";
/// The name the tools print for the broker's `MESSAGE_TOO_LARGE` error.
pub const RECORD_TOO_LARGE_EXCEPTION: &str = "RecordTooLargeException";
/// The name the tools print when the broker doesn't list a version of an API that a tool needs.
pub const UNSUPPORTED_VERSION_EXCEPTION: &str = "UnsupportedVersionException";
/// The name the tools print when the broker refuses a login.
pub const SASL_AUTHENTICATION_EXCEPTION: &str = "SaslAuthenticationException";
/// What the console producer logs on stderr for each record that the broker rejects.
const CONSOLE_PRODUCER_SEND_ERROR: &str = "Error when sending message";

pub use cluster::{ApiVersions, ClusterId};
pub use configs::{DescribeConfigs, DescribeUsers};
pub use console_consumer::{
    Consume, ConsumeMatching, ConsumedRecord, LOG_APPEND_TIME, PRINTED_NULL, PrintsPartitionLines,
};
pub use consumer_groups::{DescribeGroup, DescribeGroupState, GroupOffsets};
pub use delete_records::DeleteRecords;
pub use offsets::GetOffsets;
pub use topics::{
    CreateTopic, DeleteTopic, DescribeTopic, ListTopics, PartitionReplicas, RequiresExistingTopic,
};
pub use verifiable_producer::{Acks, VerifiableProduce, verifiable_producer_values};

/// How long a tool a test runs has to finish.
const TOOL_TIMEOUT: Duration = Duration::from_secs(60);

/// How much longer than a tool's own timeout the harness waits for
/// `docker exec` to return.
const GRACE: Duration = Duration::from_secs(15);
/// How long `timeout` waits after SIGTERM before it sends SIGKILL.
const KILL_AFTER: Duration = Duration::from_secs(5);

// The harness must wait longer than `timeout` takes to send SIGKILL, or it
// kills `docker exec` before `timeout` can report why the tool stopped.
const _: () = assert!(GRACE.as_secs() > KILL_AFTER.as_secs());

/// A SCRAM user that the tools log in as, on a broker started with `--authentication`.
#[derive(Clone, Debug)]
pub struct ScramLogin {
    pub user: String,
    pub password: String,
    pub mechanism: ScramMechanism,
}

/// A SCRAM mechanism: the hash function that a SCRAM credential and a SCRAM login use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    /// Every mechanism that Kafka supports.
    pub const ALL: [Self; 2] = [Self::Sha256, Self::Sha512];

    /// Returns the mechanism's name, as Kafka's user configs and SASL settings write it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// The Kafka CLI tools, run with `docker exec` in a container on the host
/// network, so they reach a broker at the address it advertises.
#[derive(Debug)]
pub struct KafkaCli {
    container: String,
    bootstrap: String,
    /// The client properties that every tool gets, such as the ones that log in. It is empty for
    /// a broker that doesn't require a login.
    client_properties: String,
    /// The path in the container of the file that holds `client_properties`, or `None` when
    /// `client_properties` is empty.
    client_config: Option<String>,
}

impl KafkaCli {
    /// Tools for the broker at `bootstrap`, in the `NISSHI_SMOKE_KAFKA`
    /// container that `just smoke` starts for the run.
    pub fn new(bootstrap: &str) -> Self {
        Self {
            container: settings::kafka_container(),
            bootstrap: bootstrap.to_owned(),
            client_properties: String::new(),
            client_config: None,
        }
    }

    /// Tools for the broker that all tests share, at [`settings::shared_broker_bootstrap`].
    pub fn shared() -> Self {
        Self::new(&settings::shared_broker_bootstrap())
    }

    /// Like [`KafkaCli::new`], but every tool logs in as `login`.
    ///
    /// Each tool sends `SaslHandshake` and `SaslAuthenticate` before its other requests.
    pub fn logged_in_as(bootstrap: &str, login: &ScramLogin) -> Self {
        Self::new(bootstrap).with_client_properties(&format!(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism={}\n\
             sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
             username=\"{}\" password=\"{}\";\n",
            login.mechanism.name(),
            login.user,
            login.password
        ))
    }

    /// Returns tools for the same broker whose clients give up on a call after `timeout`, instead
    /// of after the Java client's default of 60 seconds.
    pub fn with_api_timeout(&self, timeout: Duration) -> Self {
        let timeout_millis = timeout.as_millis();

        // The admin client raises `default.api.timeout.ms` to `request.timeout.ms`, 30 seconds by
        // default, when it is lower, so the tools set both.
        self.with_client_properties(&format!(
            "default.api.timeout.ms={timeout_millis}\nrequest.timeout.ms={timeout_millis}\n"
        ))
    }

    /// Returns tools for the same broker whose client config file holds these tools' client
    /// properties, followed by `properties`.
    fn with_client_properties(&self, properties: &str) -> Self {
        let client_properties = format!("{}{properties}", self.client_properties);
        let path = self.write_file_in_container("client", "properties", &client_properties);

        Self {
            container: self.container.clone(),
            bootstrap: self.bootstrap.clone(),
            client_properties,
            client_config: Some(path),
        }
    }

    /// Writes `contents` to a new file in the tools' container, and returns the file's path there.
    /// The file is named `<name_prefix>-<unique part>.<extension>`.
    fn write_file_in_container(
        &self,
        name_prefix: &str,
        extension: &str,
        contents: &str,
    ) -> String {
        let path = format!("/tmp/{}.{extension}", unique_name(name_prefix));

        let written = timed_command::run(
            Command::new("docker").args([
                "exec",
                "--interactive",
                &self.container,
                "sh",
                "-c",
                &format!("cat > {path}"),
            ]),
            Some(contents),
            TOOL_TIMEOUT,
        );

        assert!(
            matches!(&written, Ok(written) if written.code == Some(0)),
            "could not write {path} in {}: {written:?}",
            self.container
        );

        path
    }

    /// Runs `/opt/kafka/bin/<tool>.sh <args> --bootstrap-server <broker>`. The address comes last,
    /// after a subcommand such as `kafka-cluster.sh cluster-id`, which takes it as its own option.
    ///
    /// Panics if the tool does not finish within `TOOL_TIMEOUT`.
    fn run<PrintedBy>(&self, tool: Tool, args: &[&str]) -> Output<PrintedBy> {
        self.exec(tool, args, None, TOOL_TIMEOUT)
    }

    /// Like [`KafkaCli::run`], with `input` on the tool's stdin.
    fn run_with_input<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: &str,
    ) -> Output<PrintedBy> {
        self.exec(tool, args, Some(input), TOOL_TIMEOUT)
    }

    /// Waits until the broker answers [`KafkaCli::cluster_id`], and returns that answer. Returns an
    /// error soon after `check_running` reports that the broker has exited, and also if the broker
    /// does not answer within `timeout`.
    ///
    /// The harness waits in Rust until the broker's port accepts connections, and only then starts
    /// the tool. Each tool starts a JVM, which takes seconds, and a tool that can't connect keeps
    /// trying until its time limit.
    pub fn wait_until_ready(
        &self,
        timeout: Duration,
        mut check_running: impl FnMut() -> Result<(), String>,
    ) -> Result<Output<ClusterId>, String> {
        /// How long one readiness probe has to finish.
        const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
        /// How often the harness checks whether the broker has exited, and whether its port
        /// accepts connections, before it starts a tool.
        const PORT_POLL_INTERVAL: Duration = Duration::from_millis(100);

        let deadline = Instant::now() + timeout;

        loop {
            check_running()?;

            if !self.port_accepts_connections(PORT_POLL_INTERVAL) {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{} did not accept connections within {timeout:?}",
                        self.bootstrap
                    ));
                }

                thread::sleep(PORT_POLL_INTERVAL);
                continue;
            }

            // Each attempt gets at most the time left before the deadline, and at
            // least a second, because `timeout 0s` means no time limit.
            let left = deadline.saturating_duration_since(Instant::now());
            let limit = ATTEMPT_TIMEOUT.min(left).max(Duration::from_secs(1));

            let args = ["cluster-id"];
            let attempt = timed_command::run(
                &mut self.command(Tool::Cluster, &args, false, limit),
                None,
                limit + GRACE,
            );

            match attempt {
                Ok(finished) if finished.code == Some(0) => {
                    return Ok(Output::new(Tool::Cluster, &args, finished, None));
                }

                attempt if Instant::now() >= deadline => {
                    return Err(format!("did not answer within {timeout:?}: {attempt:?}"));
                }

                _ => thread::sleep(Duration::from_millis(500)),
            }
        }
    }

    fn port_accepts_connections(&self, timeout: Duration) -> bool {
        self.bootstrap
            .to_socket_addrs()
            .into_iter()
            .flatten()
            .any(|address| TcpStream::connect_timeout(&address, timeout).is_ok())
    }

    /// Runs `tool` as [`KafkaCli::run`] does, with `tool_timeout`. Panics if the tool doesn't
    /// finish in time.
    #[track_caller]
    fn exec<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: Option<&str>,
        tool_timeout: Duration,
    ) -> Output<PrintedBy> {
        let output = self.exec_until_timeout(tool, args, input, tool_timeout);

        if let Some(reason) = &output.timeout {
            panic!(
                "`{}` {reason}\nstdout:\n{}\nstderr:\n{}",
                output.command, output.stdout, output.stderr
            );
        }

        output
    }

    /// Runs `tool` as [`KafkaCli::exec`] does, but returns the output of a tool that doesn't
    /// finish in time instead of panicking, with [`Output::timeout`] set.
    fn exec_until_timeout<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: Option<&str>,
        tool_timeout: Duration,
    ) -> Output<PrintedBy> {
        /// The exit status of GNU `timeout` when it stops the tool with SIGTERM.
        const TIMEOUT_TERM_EXIT: i32 = 124;
        /// The exit status of GNU `timeout` when it sends SIGKILL, because the tool was still
        /// running `KILL_AFTER` after SIGTERM. `timeout` reports a signal as a shell does, as 128
        /// plus the signal number, and SIGKILL is 9.
        const TIMEOUT_KILL_EXIT: i32 = 128 + 9;

        let mut command = self.command(tool, args, input.is_some(), tool_timeout);

        let finished = timed_command::run(&mut command, input, tool_timeout + GRACE)
            .unwrap_or_else(|err| panic!("{err}"));

        let timeout = match finished.code {
            Some(TIMEOUT_TERM_EXIT) => Some(format!("did not finish within {tool_timeout:?}")),
            Some(TIMEOUT_KILL_EXIT) => Some(format!(
                "did not finish within {tool_timeout:?}, nor within {KILL_AFTER:?} of SIGTERM"
            )),
            _ => None,
        };

        Output::new(tool, args, finished, timeout)
    }

    fn command(&self, tool: Tool, args: &[&str], interactive: bool, timeout: Duration) -> Command {
        let mut command = Command::new("docker");
        _ = command.arg("exec");

        if interactive {
            _ = command.arg("--interactive");
        }

        // The tool runs under `timeout` in the container, because killing `docker exec` leaves
        // the tool running there, still a member of its group.
        //
        // The image needs GNU `timeout`, which exits with 124 or 137 when it stops the tool, as
        // `exec` expects. BusyBox's `timeout` exits with the tool's own status, so a timeout would
        // look like an ordinary failure.
        _ = command
            .arg(&self.container)
            .arg("timeout")
            .arg(format!("--kill-after={}s", KILL_AFTER.as_secs()))
            .arg(format!("{}s", timeout.as_secs()))
            .arg(format!("/opt/kafka/bin/{}.sh", tool.name()))
            .args(args)
            .args(["--bootstrap-server", &self.bootstrap]);

        if let Some(client_config) = &self.client_config {
            _ = command.args([tool.client_config_option(), client_config]);
        }

        command
    }
}

/// A Kafka command-line tool in `/opt/kafka/bin` that the suite runs.
#[derive(Clone, Copy, Debug)]
enum Tool {
    BrokerApiVersions,
    Cluster,
    Configs,
    ConsoleConsumer,
    ConsoleProducer,
    ConsumerGroups,
    DeleteRecords,
    GetOffsets,
    Topics,
    VerifiableProducer,
}

impl Tool {
    /// Returns the tool's name, which is also its script's file name in `/opt/kafka/bin` without
    /// `.sh`.
    fn name(self) -> &'static str {
        match self {
            Self::BrokerApiVersions => "kafka-broker-api-versions",
            Self::Cluster => "kafka-cluster",
            Self::Configs => "kafka-configs",
            Self::ConsoleConsumer => "kafka-console-consumer",
            Self::ConsoleProducer => "kafka-console-producer",
            Self::ConsumerGroups => "kafka-consumer-groups",
            Self::DeleteRecords => "kafka-delete-records",
            Self::GetOffsets => "kafka-get-offsets",
            Self::Topics => "kafka-topics",
            Self::VerifiableProducer => "kafka-verifiable-producer",
        }
    }

    /// Returns the option that gives the tool a client config file, such as the one that logs in.
    /// The tools don't share one name for it.
    fn client_config_option(self) -> &'static str {
        match self {
            Self::ConsoleProducer | Self::VerifiableProducer => "--producer.config",
            Self::ConsoleConsumer => "--consumer.config",
            Self::Cluster => "--config",
            Self::BrokerApiVersions
            | Self::Configs
            | Self::ConsumerGroups
            | Self::DeleteRecords
            | Self::GetOffsets
            | Self::Topics => "--command-config",
        }
    }
}

/// What a Kafka CLI tool printed, and how it exited. Its `Display` form shows the command, the exit
/// code, and the full stdout and stderr, for use in a failure message.
///
/// `PrintedBy` names the [`KafkaCli`] method that ran the tool, such as [`DescribeTopic`] for
/// [`KafkaCli::describe_topic`]. The readers for a command's output are methods of
/// `Output<ThatCommand>`, so the compiler rejects a reader called on another command's output.
#[derive(Debug)]
pub struct Output<PrintedBy = AnyCommand> {
    pub command: String,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    /// Why the harness stopped the tool, if it didn't finish in time.
    pub timeout: Option<String>,
    printed_by: PhantomData<PrintedBy>,
}

/// The `PrintedBy` of an [`Output`] that no reader reads, beyond its exit code and its text.
#[derive(Clone, Copy, Debug)]
pub enum AnyCommand {}

impl<PrintedBy> Output<PrintedBy> {
    fn new(
        tool: Tool,
        args: &[&str],
        finished: timed_command::Finished,
        timeout: Option<String>,
    ) -> Self {
        Self {
            command: format!("{} {}", tool.name(), args.join(" ")),
            code: finished.code,
            stdout: finished.stdout,
            stderr: finished.stderr,
            timeout,
            printed_by: PhantomData,
        }
    }

    /// Asserts the tool exited with `code`, showing its output if not.
    #[track_caller]
    pub fn exited(&self, code: i32) -> &Self {
        assert_eq!(self.code, Some(code), "{self}");
        self
    }

    /// Asserts the tool exited with 0.
    #[track_caller]
    pub fn succeeded(&self) -> &Self {
        self.exited(0)
    }

    /// Asserts that a console producer exited with 0 and did not log an error for a record. The
    /// console producer exits with 0 even if the broker rejects a record, and only logs the error.
    #[track_caller]
    fn assert_every_record_accepted(&self) {
        assert!(
            !self
                .succeeded()
                .stderr
                .contains(CONSOLE_PRODUCER_SEND_ERROR),
            "the broker rejected a record: {self}"
        );
    }

    pub fn lines(&self) -> Vec<&str> {
        self.stdout.lines().collect()
    }

    /// Returns whether the tool printed `text` on stdout or stderr. A tool prints an exception on
    /// one or the other, depending on where it caught it.
    pub fn mentions(&self, text: &str) -> bool {
        self.stdout.contains(text) || self.stderr.contains(text)
    }
}

impl<PrintedBy> fmt::Display for Output<PrintedBy> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.timeout {
            Some(reason) => write!(formatter, "`{}` {reason}", self.command)?,
            None => write!(formatter, "`{}` exited with {:?}", self.command, self.code)?,
        }

        write!(
            formatter,
            "\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        )
    }
}
