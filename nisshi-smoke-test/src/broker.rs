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

//! Finds, starts and stops the broker that a test talks to.
//!
//! A test either uses the broker that all tests share, at the address in `NISSHI_SMOKE_BOOTSTRAP`,
//! or launches a broker of its own: from the Docker image in `NISSHI_SMOKE_IMAGE` when that is
//! set, otherwise from the `nisshi` binary in `NISSHI_SMOKE_BIN`. A launched broker is checked
//! when it stops: the check fails if the broker exited or restarted before then, didn't exit with
//! 0 within 30 seconds of SIGTERM, or wrote `panicked at` to its log.

use std::{
    fs::{self, File},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU16, Ordering},
    thread,
    time::Duration,
};

use crate::{KafkaCli, env, label, timed_command, unique_name};

/// How long a launched broker has to answer after it starts.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a broker has to exit after SIGTERM.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Launched brokers listen on ports from here up. The operating system hands
/// out ports for outgoing connections and binds to port 0 from 32768 on Linux
/// and from 49152 on macOS, so it doesn't take a port in this range before the
/// broker binds it.
const FIRST_BROKER_PORT: u16 = 20000;
const BROKER_PORTS: u16 = 10000;

/// A broker the tests talk to: the shared one `just smoke` started, or one
/// of their own from [`Broker::isolated`].
#[derive(Debug)]
pub struct Broker {
    /// The broker's bootstrap address; see [`Broker::bootstrap`].
    bootstrap: String,
    /// The broker that this `Broker` launched, and so stops and checks. It is
    /// `None` for the shared broker, which `smoke-broker` stops.
    deployment: Option<Deployment>,
}

/// How to launch a broker.
#[derive(Debug)]
pub struct LaunchOptions {
    /// The port the broker listens on and advertises, on 127.0.0.1.
    pub port: u16,
    pub cluster_id: String,
    /// Extra `nisshi broker` arguments.
    pub args: Vec<String>,
    /// Where to keep the broker's log. Otherwise the log goes in the broker's temporary
    /// directory, which is removed when the broker passes its checks.
    pub log: Option<PathBuf>,
}

/// A broker that this harness launched: where it runs, and its files.
#[derive(Debug)]
struct Deployment {
    host: Host,
    dir: PathBuf,
    log: PathBuf,
}

/// What a launched broker runs in.
#[derive(Debug)]
enum Host {
    Process(Child),
    Container {
        name: String,
        volume: Option<String>,
    },
}

impl Broker {
    /// The broker that all tests share, at `NISSHI_SMOKE_BOOTSTRAP`.
    pub fn shared() -> Self {
        let bootstrap = env("NISSHI_SMOKE_BOOTSTRAP")
            .expect("NISSHI_SMOKE_BOOTSTRAP is not set: run the suite with `just smoke <engine>`");

        Self {
            bootstrap,
            deployment: None,
        }
    }

    /// A broker of this test's own, for a test that needs its own
    /// configuration or breaks its broker.
    pub fn isolated() -> Self {
        Self::isolated_with(&[])
    }

    /// Like [`Broker::isolated`], with extra `nisshi broker` arguments.
    pub fn isolated_with(args: &[&str]) -> Self {
        Self::launch(LaunchOptions {
            port: free_port(),
            cluster_id: unique_name("cluster"),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            log: None,
        })
    }

    /// Launches `NISSHI_SMOKE_IMAGE` when set, otherwise `NISSHI_SMOKE_BIN`,
    /// on `NISSHI_SMOKE_STORAGE`, waits until it answers, and checks that the
    /// broker answering has this broker's cluster id.
    ///
    /// SQLite and memory brokers get storage of their own. PostgreSQL and S3
    /// brokers share the engine's database, kept apart by cluster id.
    ///
    /// Panics if something already listens on the port, because the readiness
    /// check would then pass against that listener instead of this broker.
    pub fn launch(options: LaunchOptions) -> Self {
        assert!(
            port_is_free(options.port),
            "port {} is already in use: stop what listens there (`docker ps --filter \
             label=nisshi-smoke` lists containers an earlier run left behind), or choose \
             another port",
            options.port
        );

        let bootstrap = format!("127.0.0.1:{}", options.port);

        // The tools come first, so a failure to start them doesn't leave a
        // broker running that nothing stops.
        let cli = KafkaCli::new(&bootstrap);

        let dir = std::env::temp_dir().join(unique_name("nisshi-smoke"));
        fs::create_dir_all(&dir).expect("broker directory");

        let log = options
            .log
            .clone()
            .unwrap_or_else(|| dir.join("broker.log"));

        let storage = env("NISSHI_SMOKE_STORAGE").unwrap_or_else(|| "memory://nisshi/".to_owned());
        let image = env("NISSHI_SMOKE_IMAGE");

        let host = match image {
            Some(image) => launch_container(&image, &options, &storage),
            None => launch_process(&options, &storage, &dir, &log),
        };

        let mut broker = Self {
            bootstrap,
            deployment: Some(Deployment { host, dir, log }),
        };

        if let Some(deployment) = &mut broker.deployment {
            cli.wait_until_ready(READY_TIMEOUT, || match &mut deployment.host {
                Host::Process(child) => match child.try_wait() {
                    Ok(Some(status)) => Err(format!("exited with {status}")),
                    _ => Ok(()),
                },

                Host::Container { name, .. } => container_alive(name),
            });
        }

        // The readiness check passes for any broker on the port, and another test's broker can
        // bind the port between the check above and this broker's bind. So the harness also
        // compares the cluster id.
        let answered = cli.cluster_id();
        let expected = format!("Cluster ID: {}", options.cluster_id);

        assert!(
            answered.succeeded().lines().contains(&expected.as_str()),
            "the broker at {} is not the one launched with cluster id {}: {answered:?}",
            broker.bootstrap,
            options.cluster_id
        );

        broker
    }

    /// Returns the broker's bootstrap address, `host:port`, which [`KafkaCli::new`] takes.
    ///
    /// A Kafka client connects to its bootstrap address first, and asks that broker for the
    /// addresses of the cluster's brokers. It then connects to the address each broker
    /// advertises. Here each cluster has one broker, which listens on and advertises the
    /// same `127.0.0.1` address, so the bootstrap address is also the broker's address.
    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    /// Stops a broker this test launched, failing if it exited or restarted
    /// before now, didn't exit with 0 on SIGTERM, or logged a panic. Dropping
    /// the broker does the same.
    pub fn stop(mut self) -> Result<(), String> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<(), String> {
        let Some(deployment) = self.deployment.take() else {
            return Ok(());
        };

        let mut failures = Vec::new();

        match deployment.host {
            Host::Process(mut child) => {
                stop_process(&mut child, &mut failures);
            }

            Host::Container { name, volume } => {
                stop_container(&name, &deployment.log, &mut failures);
                remove_container(&name, volume.as_deref());
            }
        }

        let log = fs::read_to_string(&deployment.log).unwrap_or_default();

        if let Some(line) = log.lines().find(|line| line.contains("panicked at")) {
            failures.push(format!("broker panicked: {line}"));
        }

        let result = if failures.is_empty() {
            Ok(())
        } else {
            let lines = log.lines().collect::<Vec<_>>();
            let tail = lines[lines.len().saturating_sub(40)..]
                .iter()
                .map(|line| without_colour(line))
                .collect::<Vec<_>>()
                .join("\n");

            Err(format!(
                "{}\nlast lines of {}:\n{tail}",
                failures.join("\n"),
                deployment.log.display()
            ))
        };

        // A failed broker's directory stays, so the log that the error names
        // still exists.
        if result.is_ok() {
            _ = fs::remove_dir_all(&deployment.dir);
        }

        result
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let result = self.shutdown();

        // A second panic while the test is already failing would abort the
        // whole test binary and hide the first one, so the reason is printed instead.
        if let Err(reason) = result {
            if thread::panicking() {
                eprintln!("{reason}");
            } else {
                panic!("{reason}");
            }
        }
    }
}

fn broker_args(options: &LaunchOptions, storage: &str) -> Vec<String> {
    let url = format!("tcp://127.0.0.1:{}", options.port);

    let mut args = vec![
        "broker".to_owned(),
        format!("--cluster-id={}", options.cluster_id),
        format!("--listener-url={url}"),
        format!("--advertised-listener-url={url}"),
        format!("--storage-engine={storage}"),
    ];

    args.extend(options.args.iter().cloned());
    args
}

/// A broker's own SQLite database, in its working directory.
const SQLITE: &str = "sqlite://nisshi.db";

fn is_sqlite(storage: &str) -> bool {
    storage.starts_with("sqlite:")
}

fn launch_process(options: &LaunchOptions, storage: &str, dir: &Path, log: &Path) -> Host {
    let binary = env("NISSHI_SMOKE_BIN").expect(
        "set NISSHI_SMOKE_BIN or NISSHI_SMOKE_IMAGE: run the suite with `just smoke <engine>`",
    );

    // The broker runs in its own directory, where a relative path would no
    // longer find the binary.
    let binary =
        fs::canonicalize(&binary).unwrap_or_else(|err| panic!("NISSHI_SMOKE_BIN {binary}: {err}"));

    // The broker resolves a sqlite:// path against its working directory.
    let storage = if is_sqlite(storage) {
        SQLITE.to_owned()
    } else {
        storage.to_owned()
    };

    let output = File::create(log).expect("broker log");
    let errors = output.try_clone().expect("broker log");

    let mut command = Command::new(&binary);

    _ = command
        .args(broker_args(options, &storage))
        // The broker loads `.env` from its working directory; there is none
        // here.
        .current_dir(dir)
        .env_clear()
        .envs(broker_environment())
        .stdin(Stdio::null())
        .stdout(output)
        .stderr(errors);

    Host::Process(
        command
            .spawn()
            .unwrap_or_else(|err| panic!("could not start {}: {err}", binary.display())),
    )
}

fn launch_container(image: &str, options: &LaunchOptions, storage: &str) -> Host {
    let name = unique_name("nisshi-smoke-broker");

    let (storage, volume) = if is_sqlite(storage) {
        (SQLITE.to_owned(), Some(name.clone()))
    } else {
        (storage.to_owned(), None)
    };

    let mut command = Command::new("docker");

    _ = command.args([
        "run",
        "--detach",
        "--name",
        &name,
        &label(),
        "--network=host",
        // Restart a crashed broker, so a crash shows up as a restart.
        "--restart=on-failure",
    ]);

    // The image sets its own PATH and HOME.
    for (variable, _) in broker_environment() {
        if variable != "PATH" && variable != "HOME" {
            _ = command.args(["--env", &variable]);
        }
    }

    if let Some(volume) = &volume {
        _ = Command::new("docker")
            .args(["volume", "create", &label(), volume])
            .output();
        _ = command.args(["--volume", &format!("{volume}:/data"), "--workdir=/data"]);
    }

    _ = command.arg(image).args(broker_args(options, &storage));

    let started = timed_command::run(&mut command, None, Duration::from_secs(300));

    if !matches!(&started, Ok(started) if started.code == Some(0)) {
        remove_container(&name, volume.as_deref());
        panic!("could not start {image}: {started:?}");
    }

    Host::Container { name, volume }
}

/// Returns a port for a broker to listen on, which nothing listens on yet.
///
/// Panics if every port in the range is in use.
pub fn free_port() -> u16 {
    static PORTS_TRIED: AtomicU16 = AtomicU16::new(0);

    // Each process starts at its own offset, so parallel tests try different ports.
    let start = (std::process::id() % u32::from(BROKER_PORTS)) as u16;

    (0..BROKER_PORTS)
        .map(|_| {
            let offset = start.wrapping_add(PORTS_TRIED.fetch_add(1, Ordering::Relaxed));
            FIRST_BROKER_PORT + offset % BROKER_PORTS
        })
        .find(|port| port_is_free(*port))
        .expect("no free port")
}

fn port_is_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Returns the environment variables a broker is launched with: `PATH`,
/// `HOME`, the log settings, and every `AWS_` variable, which the S3 engine
/// reads its endpoint and credentials from.
///
/// The broker reads much of its configuration from environment variables,
/// and `just` loads a developer's `.env` into the environment. The broker gets
/// only these variables, so a local `.env` can't make it differ from the CI tests.
fn broker_environment() -> impl Iterator<Item = (String, String)> {
    std::env::vars().filter(|(name, _)| {
        name.starts_with("AWS_")
            || ["HOME", "PATH", "RUST_BACKTRACE", "RUST_LOG"].contains(&name.as_str())
    })
}

fn stop_process(child: &mut Child, failures: &mut Vec<String>) {
    if let Ok(Some(status)) = child.try_wait() {
        failures.push(format!("broker exited during the run with {status}"));
        return;
    }

    _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();

    match timed_command::wait(child, STOP_TIMEOUT) {
        Some(status) if status.success() => {}
        Some(status) => failures.push(format!("broker exited with {status} on SIGTERM")),
        None => failures.push(format!(
            "broker did not exit within {STOP_TIMEOUT:?} of SIGTERM"
        )),
    }
}

fn container_alive(name: &str) -> Result<(), String> {
    let state = inspect(name, "{{.State.Running}} {{.RestartCount}}")?;

    match state.as_str() {
        "true 0" => Ok(()),
        state => Err(format!(
            "container {name} running and restart count: {state}"
        )),
    }
}

fn stop_container(name: &str, log: &Path, failures: &mut Vec<String>) {
    if let Err(reason) = container_alive(name) {
        failures.push(format!(
            "broker exited or restarted during the run: {reason}"
        ));
    }

    let stopped = timed_command::run(
        Command::new("docker").args(["stop", &format!("--time={}", STOP_TIMEOUT.as_secs()), name]),
        None,
        STOP_TIMEOUT + Duration::from_secs(30),
    );

    if let Err(reason) = stopped {
        failures.push(reason);
    }

    match inspect(name, "{{.State.ExitCode}}") {
        Ok(code) if code == "0" => {}
        Ok(code) => failures.push(format!("broker exited with {code} on SIGTERM")),
        Err(reason) => failures.push(reason),
    }

    if let Ok(logs) = Command::new("docker").args(["logs", name]).output() {
        let mut text = logs.stdout;
        text.extend(logs.stderr);
        _ = fs::write(log, text);
    }
}

/// Removes a broker container, with its SQLite volume if it has one.
fn remove_container(name: &str, volume: Option<&str>) {
    _ = Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .output();

    if let Some(volume) = volume {
        _ = Command::new("docker")
            .args(["volume", "rm", "--force", volume])
            .output();
    }
}

fn inspect(name: &str, format: &str) -> Result<String, String> {
    let output = Command::new("docker")
        .args(["inspect", "--format", format, name])
        .output()
        .map_err(|err| format!("docker inspect {name}: {err}"))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(format!(
            "docker inspect {name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Removes the terminal colour codes the broker writes into its log.
fn without_colour(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut chars = line.chars();

    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // An SGR sequence: ESC, '[', parameters, then 'm'.
            for c in chars.by_ref() {
                if c == 'm' {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }

    plain
}

#[cfg(test)]
mod tests {
    use super::without_colour;

    #[test]
    fn colour_codes_are_removed() {
        assert_eq!(
            without_colour("\u{1b}[2m2026\u{1b}[0m \u{1b}[34mDEBUG\u{1b}[0m ready"),
            "2026 DEBUG ready"
        );
    }
}
