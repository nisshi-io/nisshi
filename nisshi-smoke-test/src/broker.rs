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
//! when it stops: the check fails if the broker exited before then, didn't exit with
//! 0 within 30 seconds of SIGTERM, or wrote `panicked at` to its log.
//!
//! A test can restart a launched broker on the same storage and port. The harness first stops the
//! broker with SIGTERM, or kills it with SIGKILL. A test that expects the broker to refuse its
//! configuration gets a [`FailedStart`] instead of a broker.
//!
//! Each launched broker keeps its files, and a log per start, in a directory of its own under
//! `NISSHI_SMOKE_WORK_DIR`. The directory is removed when the broker passes its checks and the
//! test hasn't failed.

use std::{
    collections::VecDeque,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU16, Ordering},
    thread,
    time::{Duration, SystemTime},
};

use crate::{KafkaCli, ScramLogin, StorageUrl, label, settings, timed_command, unique_name};

/// How long a broker has to exit after SIGTERM.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long `docker run` has to start a broker container. `run.sh` pulls the image before the
/// tests run, so this covers only creating and starting the container, and stays well under the
/// time nextest gives a test before it kills it.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(60);

/// The file in a process broker's directory that holds its process id until the harness has
/// stopped it. nextest kills a test that runs too long with SIGKILL, which skips `Drop` and leaves
/// the broker running; `run.sh` stops every broker whose file is still there when the run ends.
const PID_FILE: &str = "broker.pid";

/// The storage URL option that makes a broker run maintenance every 2 seconds instead of every 10
/// minutes, for a test that waits for a maintenance run.
pub const FREQUENT_MAINTENANCE: &str = "maintenance_interval=2s";

/// What `docker cp` prints when the file to copy isn't in the container.
const DOCKER_CP_MISSING_FILE: &str = "Could not find the file";

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
    /// The port the broker listens on and advertises, on 127.0.0.1. The broker keeps the port
    /// when it restarts.
    pub port: ReservedPort,
    pub cluster_id: String,
    /// The user every client logs in as. When it is set, the broker runs with `--authentication`,
    /// and the harness's own tools log in as this user.
    pub login: Option<ScramLogin>,
    /// Where to keep the broker's log. Otherwise the log goes in the broker's temporary
    /// directory, which is removed when the broker passes its checks.
    pub log: Option<PathBuf>,
    /// The storage engine URL, usually [`settings::storage_url_under_test`].
    pub storage: StorageUrl,
}

impl LaunchOptions {
    /// Launch options for a broker on `storage`. The broker gets a cluster id of its own, so its
    /// data stays apart from other brokers' data on a shared storage engine.
    pub fn new(storage: StorageUrl) -> Self {
        Self {
            port: reserve_port(),
            cluster_id: unique_name("cluster"),
            login: None,
            log: None,
            storage,
        }
    }
}

/// How the harness stops a broker.
#[derive(Clone, Copy, Debug)]
enum Shutdown {
    /// SIGTERM, after which the broker must exit with 0 within [`STOP_TIMEOUT`].
    Graceful,
    /// SIGKILL. The broker stops at once, as in a crash, and can't finish a write in progress.
    Kill,
}

/// A broker that this harness launched.
#[derive(Debug)]
struct Deployment {
    host: Host,
    files: BrokerFiles,
    log: PathBuf,
    options: LaunchOptions,
}

/// What a launched broker runs in.
#[derive(Debug)]
enum Host {
    Process(Child),
    Container { name: String },
}

/// A launched broker's files, which it keeps when it restarts. Dropping them removes the volume,
/// and also the directory unless [`BrokerFiles::keep_dir`] is set or the test is failing,
/// because the directory then holds the logs that show why.
#[derive(Debug)]
struct BrokerFiles {
    /// The broker's own directory: a log per start, and a process's SQLite database.
    dir: PathBuf,
    /// A container's SQLite database volume, mounted as its working directory.
    volume: Option<String>,
    /// How many times the broker has started, which numbers its logs.
    starts: u32,
    /// Keeps the directory after the files are dropped, for a broker that failed its checks.
    keep_dir: bool,
}

impl Broker {
    /// A broker of this test's own, for a test that needs its own configuration or breaks its
    /// broker. It runs on [`settings::storage_url_under_test`].
    pub fn isolated() -> Self {
        Self::launch(LaunchOptions::new(settings::storage_url_under_test()))
    }

    /// Launches a broker of this test's own, as [`Broker::isolated`] does, that runs maintenance
    /// every 2 seconds instead of every 10 minutes, for a test that waits for a maintenance run.
    pub fn isolated_with_frequent_maintenance() -> Self {
        Self::launch(LaunchOptions::new(
            settings::storage_url_under_test().with_query_option(FREQUENT_MAINTENANCE),
        ))
    }

    /// Launches `NISSHI_SMOKE_IMAGE` when set, otherwise `NISSHI_SMOKE_BIN`, and waits until it
    /// answers. Panics if it doesn't start, or if something else already listens on the port.
    pub fn launch(options: LaunchOptions) -> Self {
        let cluster_id = options.cluster_id.clone();

        Self::try_launch(options).unwrap_or_else(|failed_start| {
            panic!("the broker with cluster id {cluster_id} didn't start: {failed_start}")
        })
    }

    /// Like [`Broker::launch`], but returns a [`FailedStart`] when the broker exits or doesn't
    /// answer, for a test that expects the broker to refuse its configuration. Panics, as `launch`
    /// does, when the binary or container can't start at all.
    fn try_launch(options: LaunchOptions) -> Result<Self, FailedStart> {
        assert_port_is_free(options.port.number());

        let files = BrokerFiles::new(&options.storage);
        Self::start_with_files(options, files)
    }

    /// Launches a broker that must refuse its configuration, and returns how it exited. Panics if
    /// the broker starts, exits with 0, or panics, because a broker that panics gives the user
    /// only a backtrace.
    pub fn launch_expecting_refusal(options: LaunchOptions) -> FailedStart {
        let refused = match Self::try_launch(options) {
            Ok(broker) => panic!("the broker started at {}", broker.bootstrap()),
            Err(refused) => refused,
        };

        assert!(
            refused.exit_code.is_some_and(|code| code != 0) && !refused.log.contains("panicked at"),
            "the broker didn't exit with an error: {refused}"
        );

        refused
    }

    /// Stops the broker as [`Broker::stop`] does, and starts it again on the same storage and
    /// port. Panics if it fails its checks or doesn't start again.
    pub fn restart(self) -> Self {
        self.restart_with(Shutdown::Graceful, |_| {}, &[] as &[&str])
    }

    /// Kills the broker with SIGKILL, as a crash would, and restarts it on the same storage and
    /// port. Panics if the broker had already exited, logged a panic, or doesn't start again.
    pub fn kill_and_restart(self) -> Self {
        self.restart_with(Shutdown::Kill, |_| {}, &[] as &[&str])
    }

    /// Like [`Broker::restart`], but the broker starts again with `--authentication`, so every
    /// client must log in. The harness's readiness check logs in as `login`, so the broker starts
    /// again only if it still has that user's credentials.
    pub fn restart_requiring_login(self, login: &ScramLogin) -> Self {
        self.restart_with(
            Shutdown::Graceful,
            |options| options.login = Some(login.clone()),
            &[] as &[&str],
        )
    }

    /// Like [`Broker::restart`], but the broker starts again on `storage`, with the same cluster
    /// id and working directory. A relative SQLite path in `storage` therefore names a file next
    /// to the broker's own database.
    ///
    /// Each of `files_to_delete`, a path relative to the working directory, is deleted while the
    /// broker is stopped, so a test can make sure that the broker can't read them after the
    /// restart. A file that doesn't exist is skipped.
    pub fn restart_on_storage(
        self,
        storage: StorageUrl,
        files_to_delete: &[impl AsRef<str>],
    ) -> Self {
        self.restart_with(
            Shutdown::Graceful,
            |options| options.storage = storage,
            files_to_delete,
        )
    }

    fn restart_with(
        mut self,
        shutdown: Shutdown,
        change_options: impl FnOnce(&mut LaunchOptions),
        files_to_delete: &[impl AsRef<str>],
    ) -> Self {
        let deployment = self
            .deployment
            .take()
            .expect("only a broker this test launched can restart");

        let (stopped, mut files, mut options) = deployment.stop(shutdown);

        if let Err(reason) = stopped {
            panic!("{reason}");
        }

        files.delete(files_to_delete);
        change_options(&mut options);
        files.create_volume_for(&options.storage);
        let cluster_id = options.cluster_id.clone();

        Self::start_with_files(options, files).unwrap_or_else(|failed_start| {
            panic!("the broker with cluster id {cluster_id} didn't start again: {failed_start}")
        })
    }

    fn start_with_files(
        options: LaunchOptions,
        mut files: BrokerFiles,
    ) -> Result<Self, FailedStart> {
        let bootstrap = format!("127.0.0.1:{}", options.port.number());

        // The harness finds the tools' container before it launches the broker, so a missing
        // `NISSHI_SMOKE_KAFKA` panics while no broker is running.
        let cli = match &options.login {
            Some(login) => KafkaCli::logged_in_as(&bootstrap, login),
            None => KafkaCli::new(&bootstrap),
        };

        let log = options.log.clone().unwrap_or_else(|| files.next_log());

        let host = match settings::broker_image() {
            Some(image) => launch_container(&image, &options, files.volume.as_deref()),
            None => launch_process(&options, &files.dir, &log),
        };

        let host = host.unwrap_or_else(|reason| panic!("{reason}"));

        // nextest shows a test's stderr only when the test fails, or when nextest kills it for
        // running too long. That kill skips `Drop`, so this line is the only place that names the
        // test's broker.
        eprintln!(
            "launched the broker with cluster id {} on port {}{}, logging to {}",
            options.cluster_id,
            options.port.number(),
            match &host {
                Host::Process(child) => format!(" as process {}", child.id()),
                Host::Container { name } => format!(" in container {name}"),
            },
            log.display()
        );

        /// How long a launched broker has to answer after it starts.
        const READY_TIMEOUT: Duration = Duration::from_secs(60);

        let mut deployment = Deployment {
            host,
            files,
            log,
            options,
        };

        let answered = match cli.wait_until_ready(READY_TIMEOUT, || deployment.host.check_running())
        {
            Ok(answered) => answered,
            Err(reason) => return Err(deployment.stop_and_collect_failed_start(reason)),
        };

        let expected = deployment.options.cluster_id.clone();

        let broker = Self {
            bootstrap,
            deployment: Some(deployment),
        };

        // The harness also checks the cluster id in the answer. Another test's broker can bind
        // this port after `assert_port_is_free` and before this broker does, and any broker on the
        // port answers the readiness check.
        assert_eq!(
            answered.cluster_id(),
            Some(expected.as_str()),
            "the broker at {} is not the one launched with cluster id {expected}: {answered}",
            broker.bootstrap,
        );

        Ok(broker)
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

    /// Returns whether the file at `path` exists where the broker runs: on this machine for a
    /// broker process, and in its container for a broker container. A relative `path` starts at
    /// the broker's working directory.
    ///
    /// Panics for the shared broker. `smoke-broker` launches that broker, so this `Broker` doesn't
    /// know where the broker runs.
    pub fn file_exists(&self, path: &str) -> bool {
        self.launched_deployment().file_metadata(path).is_some()
    }

    /// Returns when the file at `path`, where the broker runs, was last written, or `None` if the
    /// file doesn't exist. A relative `path` starts at the broker's working directory.
    ///
    /// Panics for the shared broker. `smoke-broker` launches that broker, so this `Broker` doesn't
    /// know where the broker runs.
    pub fn file_modification_time(&self, path: &str) -> Option<SystemTime> {
        self.launched_deployment()
            .file_metadata(path)?
            .modified()
            .ok()
    }

    /// Returns the broker that this `Broker` launched. Panics for the shared broker.
    fn launched_deployment(&self) -> &Deployment {
        self.deployment
            .as_ref()
            .expect("only a broker this test launched has files the test can check")
    }

    /// Stops a broker this test launched, failing if it exited before now, didn't exit with 0 on
    /// SIGTERM, or logged a panic. Dropping the broker does the same.
    pub fn stop(mut self) -> Result<(), StopFailure> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<(), StopFailure> {
        let Some(deployment) = self.deployment.take() else {
            return Ok(());
        };

        let (stopped, mut files, _) = deployment.stop(Shutdown::Graceful);

        if stopped.is_err() {
            files.keep_dir = true;
        }

        stopped
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

/// A broker that didn't start: how it was launched, how it exited, and what it printed.
#[derive(Debug)]
pub struct FailedStart {
    /// The `nisshi broker` command line.
    command: String,
    /// The broker's exit code, or `None` when the harness killed a broker that didn't answer.
    exit_code: Option<i32>,
    /// What the broker printed, stdout and stderr together.
    log: String,
    /// Why the harness counts the start as failed.
    reason: String,
}

impl FailedStart {
    /// Returns the error lines the broker printed: each line that starts with `Error: ` or
    /// `error: `, or that tracing logged at level `ERROR`.
    ///
    /// A test checks these lines rather than the whole log, because the broker prints its storage
    /// URL at startup. A bad value in the URL is therefore in the log whatever the error says.
    fn error_lines(&self) -> impl Iterator<Item = &str> {
        self.log.lines().filter(|line| {
            line.starts_with("Error: ")
                || line.starts_with("error: ")
                || line.split_whitespace().nth(1) == Some("ERROR")
        })
    }

    /// Asserts the broker exited with an error line that contains `text`.
    #[track_caller]
    pub fn assert_error_names(&self, text: &str) {
        assert!(
            self.error_lines().any(|line| line.contains(text)),
            "the broker didn't exit with an error that names {text}: {self}"
        );
    }
}

impl fmt::Display for FailedStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exit_code = self.exit_code.map_or_else(
            || "none, the harness killed it".to_owned(),
            |code| code.to_string(),
        );

        write!(
            formatter,
            "`{}` didn't start: {}\nexit code: {exit_code}\nlog:\n{}",
            self.command, self.reason, self.log
        )
    }
}

impl Deployment {
    /// Returns the metadata of the file at `path` where the broker runs, or `None` if the file
    /// doesn't exist. A relative `path` starts at the broker's working directory.
    ///
    /// Panics if the harness can't read the file for another reason, so that a test never takes
    /// such a failure for a missing file.
    fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        match &self.host {
            Host::Process(_) => match fs::metadata(self.files.dir.join(path)) {
                Ok(metadata) => Some(metadata),
                Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                Err(err) => panic!("{path}: {err}"),
            },

            Host::Container { name } => {
                // The broker's image holds only the broker, without a shell or `stat`, so the
                // harness copies the file out with `docker cp`, which keeps its modification time.
                // `docker cp` reads a relative path from the container's root, not from its
                // working directory.
                let working_dir = inspect(name, "{{.Config.WorkingDir}}")
                    .unwrap_or_else(|reason| panic!("{reason}"));
                let path_in_container = Path::new("/").join(working_dir).join(path);

                let copy = self.files.dir.join(unique_name("copied"));
                let source = format!("{name}:{}", path_in_container.display());

                let copied = docker(&["cp", &source, &copy.display().to_string()])
                    .unwrap_or_else(|reason| panic!("{reason}"));

                let metadata = match copied.code {
                    Some(0) => Some(
                        fs::metadata(&copy)
                            .unwrap_or_else(|err| panic!("{}: {err}", copy.display())),
                    ),
                    _ if copied.stderr.contains(DOCKER_CP_MISSING_FILE) => None,
                    _ => panic!("docker cp {source}: {}", copied.stderr.trim()),
                };

                _ = fs::remove_file(&copy);
                metadata
            }
        }
    }

    /// Stops the broker and checks it. Returns the files and options too, for a restart.
    fn stop(self, shutdown: Shutdown) -> (Result<(), StopFailure>, BrokerFiles, LaunchOptions) {
        let mut reasons = Vec::new();

        match self.host {
            Host::Process(mut child) => {
                stop_process(&mut child, shutdown, &mut reasons);
                _ = fs::remove_file(self.files.dir.join(PID_FILE));
            }

            Host::Container { name } => {
                stop_container(&name, shutdown, &self.log, &mut reasons);
                remove_container(&name);
            }
        }

        let mut last_log_lines = VecDeque::with_capacity(StopFailure::QUOTED_LOG_LINES);

        match first_panic_in_log(&self.log, &mut last_log_lines) {
            Ok(Some(panic)) => reasons.push(format!("broker panicked: {panic}")),
            Ok(None) => {}
            Err(err) => reasons.push(format!(
                "couldn't read {} to check for a panic: {err}",
                self.log.display()
            )),
        }

        let result = if reasons.is_empty() {
            Ok(())
        } else {
            Err(StopFailure {
                reasons,
                log: self.log.clone(),
                last_log_lines,
            })
        };

        (result, self.files, self.options)
    }

    /// Stops a broker that didn't start, if it is still running, and removes it and its files. The
    /// returned [`FailedStart`] holds its whole log, so nothing is lost.
    fn stop_and_collect_failed_start(mut self, reason: String) -> FailedStart {
        let code = match &mut self.host {
            Host::Process(child) => {
                if let Ok(None) = child.try_wait() {
                    _ = child.kill();
                }

                let code = child.wait().ok().and_then(|status| status.code());
                _ = fs::remove_file(self.files.dir.join(PID_FILE));
                code
            }

            Host::Container { name } => {
                let code = match check_container_running(name) {
                    Ok(()) => {
                        _ = docker(&["kill", name]);
                        None
                    }

                    Err(_) => inspect(name, "{{.State.ExitCode}}")
                        .ok()
                        .and_then(|code| code.parse().ok()),
                };

                // Save the container's output before removing the container deletes it.
                // `FailedStart` reads the broker's error from this log, so without it a test
                // that expects a refusal fails.
                _ = save_logs(name, &self.log);
                remove_container(name);
                code
            }
        };

        let log = fs::read_to_string(&self.log).unwrap_or_default();

        FailedStart {
            command: format!("nisshi {}", broker_args(&self.options).join(" ")),
            exit_code: code,
            log: log
                .lines()
                .map(|line| without_colour(line) + "\n")
                .collect(),
            reason,
        }
    }
}

impl Host {
    /// An error if the broker has exited.
    fn check_running(&mut self) -> Result<(), String> {
        match self {
            Self::Process(child) => match child.try_wait() {
                Ok(Some(status)) => Err(format!("exited with {status}")),
                _ => Ok(()),
            },

            Self::Container { name } => check_container_running(name),
        }
    }
}

impl BrokerFiles {
    /// Creates a directory under [`settings::broker_work_dir`], and a volume for a SQLite broker in
    /// a container.
    fn new(storage: &StorageUrl) -> Self {
        let dir = settings::broker_work_dir().join(unique_name("nisshi-smoke"));
        fs::create_dir_all(&dir)
            .unwrap_or_else(|err| panic!("broker directory {}: {err}", dir.display()));

        let mut files = Self {
            dir,
            volume: None,
            starts: 0,
            keep_dir: false,
        };
        files.create_volume_for(storage);

        files
    }

    /// Creates a volume for a SQLite broker in a container, if the files don't have one yet.
    ///
    /// The volume is the container's working directory, where the broker resolves a relative
    /// `sqlite://` path. A database outside the volume is removed with the container, at the next
    /// stop.
    fn create_volume_for(&mut self, storage: &StorageUrl) {
        if self.volume.is_none() && settings::broker_image().is_some() && storage.is_sqlite() {
            let volume = unique_name("nisshi-smoke-sqlite");

            _ = docker(&["volume", "create", &label(), &volume]);

            self.volume = Some(volume);
        }
    }

    /// Deletes each of `paths`, relative to the broker's working directory, while the broker is
    /// stopped. Skips a path that doesn't exist, and panics if a deletion fails.
    ///
    /// A broker container without a volume keeps its files in the container, which the harness
    /// removed when the broker stopped, so there is nothing to delete.
    fn delete(&self, paths: &[impl AsRef<str>]) {
        if paths.is_empty() {
            return;
        }

        match (&self.volume, settings::broker_image()) {
            (_, None) => {
                for path in paths {
                    let path = path.as_ref();
                    match fs::remove_file(self.dir.join(path)) {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        Err(err) => panic!("{path}: {err}"),
                    }
                }
            }

            (Some(volume), Some(_)) => {
                // The broker's image holds only the broker, without `rm`, so a container from the
                // Kafka tools' image deletes the files from the volume.
                let tools_image = inspect(&settings::kafka_container(), "{{.Config.Image}}")
                    .unwrap_or_else(|reason| panic!("{reason}"));
                let mount = format!("{volume}:/data");
                let label = label();
                let mut args = vec![
                    "run",
                    "--rm",
                    &label,
                    // The broker writes the volume as root. The tools' image runs as a user that
                    // can't delete root's files.
                    "--user=0",
                    "--volume",
                    &mount,
                    "--entrypoint",
                    "rm",
                    &tools_image,
                    "-f",
                ];
                let paths_in_volume = paths
                    .iter()
                    .map(|path| format!("/data/{}", path.as_ref()))
                    .collect::<Vec<_>>();
                args.extend(paths_in_volume.iter().map(String::as_str));

                match docker(&args) {
                    Ok(deleted) if deleted.code == Some(0) => {}
                    deleted => {
                        panic!("couldn't delete {paths_in_volume:?} from {volume}: {deleted:?}")
                    }
                }
            }

            (None, Some(_)) => {}
        }
    }

    /// The log for the broker's next start, so a restart keeps the log from before it.
    fn next_log(&mut self) -> PathBuf {
        self.starts += 1;
        self.dir.join(format!("broker-{}.log", self.starts))
    }
}

impl Drop for BrokerFiles {
    fn drop(&mut self) {
        if let Some(volume) = &self.volume {
            _ = docker(&["volume", "rm", "--force", volume]);
        }

        if !self.keep_dir && !thread::panicking() {
            _ = fs::remove_dir_all(&self.dir);
        }
    }
}

fn broker_args(options: &LaunchOptions) -> Vec<String> {
    let url = format!("tcp://127.0.0.1:{}", options.port.number());

    let mut args = vec![
        "broker".to_owned(),
        format!("--cluster-id={}", options.cluster_id),
        format!("--listener-url={url}"),
        format!("--advertised-listener-url={url}"),
        format!("--storage-engine={}", options.storage),
    ];

    if options.login.is_some() {
        args.push("--authentication".to_owned());
    }

    args
}

/// Why a broker failed its checks when it stopped, with the last lines of its log, which show what
/// the broker was doing.
#[derive(Debug)]
pub struct StopFailure {
    reasons: Vec<String>,
    log: PathBuf,
    last_log_lines: VecDeque<String>,
}

impl StopFailure {
    /// How many of the broker log's last lines a stop failure quotes.
    const QUOTED_LOG_LINES: usize = 40;
}

impl fmt::Display for StopFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "{}", self.reasons.join("\n"))?;
        write!(formatter, "last lines of {}:", self.log.display())?;

        for line in &self.last_log_lines {
            write!(formatter, "\n{}", without_colour(line))?;
        }

        Ok(())
    }
}

/// Returns the first line of the broker's log at `log` that reports a panic, and keeps the log's
/// last [`StopFailure::QUOTED_LOG_LINES`] lines in `last_log_lines`. The function reads the log
/// one line at a time instead of all at once, because a broker that logs at debug level writes
/// gigabytes in one run.
fn first_panic_in_log(
    log: &Path,
    last_log_lines: &mut VecDeque<String>,
) -> io::Result<Option<String>> {
    let mut reader = BufReader::new(File::open(log)?);
    let mut first_panic = None;
    let mut bytes = Vec::new();

    while reader.read_until(b'\n', &mut bytes)? > 0 {
        // The broker can log bytes that aren't UTF-8, such as a record's value.
        let line = String::from_utf8_lossy(&bytes).trim_end().to_owned();
        bytes.clear();

        if first_panic.is_none() && line.contains("panicked at") {
            first_panic = Some(line.clone());
        }

        if last_log_lines.len() == StopFailure::QUOTED_LOG_LINES {
            _ = last_log_lines.pop_front();
        }
        last_log_lines.push_back(line);
    }

    Ok(first_panic)
}

fn launch_process(options: &LaunchOptions, dir: &Path, log: &Path) -> Result<Host, String> {
    let binary = settings::broker_binary();

    // The broker runs in its own directory, where a relative path would no
    // longer find the binary.
    let binary =
        fs::canonicalize(&binary).unwrap_or_else(|err| panic!("NISSHI_SMOKE_BIN {binary}: {err}"));

    let output = File::create(log).expect("broker log");
    let errors = output.try_clone().expect("broker log");

    let mut command = Command::new(&binary);

    _ = command
        .args(broker_args(options))
        // The broker resolves a relative sqlite:// path against its working
        // directory, and loads `.env` from it; there is none here.
        .current_dir(dir)
        .env_clear()
        .envs(broker_environment())
        .stdin(Stdio::null())
        .stdout(output)
        .stderr(errors);

    let child = command
        .spawn()
        .map_err(|err| format!("could not start {}: {err}", binary.display()))?;

    fs::write(dir.join(PID_FILE), child.id().to_string())
        .unwrap_or_else(|err| panic!("{}: {err}", dir.join(PID_FILE).display()));

    Ok(Host::Process(child))
}

/// Starts `image` in a container, with a SQLite broker's `volume` as its working directory.
fn launch_container(
    image: &str,
    options: &LaunchOptions,
    volume: Option<&str>,
) -> Result<Host, String> {
    let name = unique_name("nisshi-smoke-broker");

    let mut command = Command::new("docker");

    _ = command.args([
        "run",
        "--detach",
        "--name",
        &name,
        &label(),
        "--network=host",
    ]);

    // The image sets its own PATH and HOME.
    for (variable, _) in broker_environment() {
        if variable != "PATH" && variable != "HOME" {
            _ = command.args(["--env", &variable]);
        }
    }

    if let Some(volume) = volume {
        _ = command.args(["--volume", &format!("{volume}:/data"), "--workdir=/data"]);
    }

    _ = command.arg(image).args(broker_args(options));

    let started = timed_command::run(&mut command, None, CONTAINER_START_TIMEOUT);

    if !matches!(&started, Ok(started) if started.code == Some(0)) {
        remove_container(&name);
        return Err(format!("could not start {image}: {started:?}"));
    }

    Ok(Host::Container { name })
}

/// A port that one broker or PostgreSQL server owns. While the `ReservedPort` exists,
/// `reserve_port` skips its port for every other test of every smoke run of the same user, so the
/// owner can stop and start again on it.
///
/// A port that its stopped owner keeps looks free to a check that something listens on it. So the
/// reservation is an exclusive `flock` on a file named after the port, in
/// `settings::port_lock_dir`, which every test process of the user sees. nextest runs each test in
/// a process of its own, so a lock inside one process would not reach the other tests. The
/// operating system releases the lock when the process exits, so a test that nextest kills leaves
/// no reservation behind.
#[derive(Debug)]
pub struct ReservedPort {
    number: u16,
    _lock: File,
}

impl ReservedPort {
    pub fn number(&self) -> u16 {
        self.number
    }
}

/// Reserves a port that is free, and that no other test on this machine has reserved.
///
/// Panics if every port in the range is in use or reserved.
pub(crate) fn reserve_port() -> ReservedPort {
    candidate_ports()
        .find_map(|port| {
            let lock = try_lock_port(port)?;

            // A process outside the smoke runs can still listen on a reserved port.
            port_is_free(port).then_some(ReservedPort {
                number: port,
                _lock: lock,
            })
        })
        .expect("no free port")
}

/// Takes the lock file of `port`, or returns `None` if another `ReservedPort` holds it.
fn try_lock_port(port: u16) -> Option<File> {
    let dir = settings::port_lock_dir();
    fs::create_dir_all(&dir).unwrap_or_else(|err| panic!("{}: {err}", dir.display()));

    let path = dir.join(port.to_string());
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .unwrap_or_else(|err| panic!("{}: {err}", path.display()));

    file.try_lock().ok().map(|()| file)
}

/// Returns the ports that [`reserve_port`] tries, in order.
fn candidate_ports() -> impl Iterator<Item = u16> {
    /// Reserved ports start here, below the ports that the operating system picks for outgoing
    /// connections and port-0 binds: 32768 up on Linux, 49152 up on macOS. The operating system
    /// then can't take a reserved port before its owner binds it.
    const FIRST_BROKER_PORT: u16 = 20000;
    const BROKER_PORT_COUNT: u16 = 10000;

    static PORTS_TRIED: AtomicU16 = AtomicU16::new(0);

    // Each process starts at its own offset, so parallel tests try different ports.
    let start = (std::process::id() % u32::from(BROKER_PORT_COUNT)) as u16;

    (0..BROKER_PORT_COUNT).map(move |_| {
        let offset = start.wrapping_add(PORTS_TRIED.fetch_add(1, Ordering::Relaxed));
        FIRST_BROKER_PORT + offset % BROKER_PORT_COUNT
    })
}

/// Panics if something already listens on `port`, because the broker's readiness check would
/// then pass against it.
fn assert_port_is_free(port: u16) {
    assert!(
        port_is_free(port),
        "port {port} is already in use: stop what listens there (`docker ps --filter \
         label=nisshi-smoke` lists containers an earlier run left behind), or choose another port"
    );
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

/// Stops a broker process with SIGTERM and checks that it exits cleanly in time, or kills it with
/// SIGKILL and waits for it to exit.
fn stop_process(child: &mut Child, shutdown: Shutdown, failures: &mut Vec<String>) {
    if let Ok(Some(status)) = child.try_wait() {
        failures.push(format!("broker exited during the run with {status}"));
        return;
    }

    match shutdown {
        Shutdown::Graceful => {
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

        Shutdown::Kill => {
            if let Err(err) = child.kill() {
                failures.push(format!("couldn't kill the broker: {err}"));
            }

            _ = child.wait();
        }
    }
}

fn check_container_running(name: &str) -> Result<(), String> {
    match inspect(name, "{{.State.Running}} {{.State.ExitCode}}")?.split_once(' ') {
        Some(("true", _)) => Ok(()),
        Some((_, code)) => Err(format!("exited with {code}")),
        None => Err(format!("container {name} has no state")),
    }
}

/// Stops a broker container like [`stop_process`], and saves what it printed to `log`.
fn stop_container(name: &str, shutdown: Shutdown, log: &Path, failures: &mut Vec<String>) {
    if let Err(reason) = check_container_running(name) {
        failures.push(format!("broker {reason} during the run"));
        failures.extend(save_logs(name, log).err());
        return;
    }

    match shutdown {
        Shutdown::Graceful => {
            let stopped = timed_command::run(
                Command::new("docker").args([
                    "stop",
                    &format!("--time={}", STOP_TIMEOUT.as_secs()),
                    name,
                ]),
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
        }

        Shutdown::Kill => match docker(&["kill", "--signal=KILL", name]) {
            Ok(killed) if killed.code == Some(0) => {}
            killed => failures.push(format!("couldn't kill {name}: {killed:?}")),
        },
    }

    failures.extend(save_logs(name, log).err());
}

/// Writes what a broker container printed to `log`.
fn save_logs(name: &str, log: &Path) -> Result<(), String> {
    let logs = docker(&["logs", name])?;

    if logs.code != Some(0) {
        return Err(format!("docker logs {name}: {}", logs.stderr.trim()));
    }

    fs::write(log, logs.stdout + logs.stderr.as_str())
        .map_err(|err| format!("couldn't write {}: {err}", log.display()))
}

/// Removes a container that the harness started, but not a broker's volume, which [`BrokerFiles`]
/// removes.
fn remove_container(name: &str) {
    _ = docker(&["rm", "--force", "--volumes", name]);
}

fn inspect(name: &str, format: &str) -> Result<String, String> {
    match docker(&["inspect", "--format", format, name])? {
        inspected if inspected.code == Some(0) => Ok(inspected.stdout.trim().to_owned()),
        inspected => Err(format!(
            "docker inspect {name}: {}",
            inspected.stderr.trim()
        )),
    }
}

/// Runs a short `docker` command, such as `docker inspect`, and kills it if it hangs.
pub(crate) fn docker(args: &[&str]) -> Result<timed_command::Finished, String> {
    timed_command::run(
        Command::new("docker").args(args),
        None,
        Duration::from_secs(60),
    )
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
    use std::{collections::VecDeque, env, fs, path::PathBuf};

    use super::{FailedStart, StopFailure, first_panic_in_log, without_colour};
    use crate::unique_name;

    /// Writes `log` to a new file in the system's temporary directory, and returns its path.
    fn log_file(log: &str) -> PathBuf {
        let path = env::temp_dir().join(format!("{}.log", unique_name("broker")));
        fs::write(&path, log).expect("the test writes its log file");
        path
    }

    #[test]
    fn clean_log_has_no_panic() {
        let log = log_file("started\nstopped\n");
        let mut last_log_lines = VecDeque::new();

        assert_eq!(
            first_panic_in_log(&log, &mut last_log_lines).expect("the test reads its log file"),
            None
        );
        assert_eq!(last_log_lines, ["started", "stopped"]);

        _ = fs::remove_file(log);
    }

    #[test]
    fn first_panic_is_found_and_only_the_last_lines_are_kept() {
        let mut lines = vec!["thread 'a' panicked at src/a.rs:1:1:".to_owned()];
        lines.extend((0..StopFailure::QUOTED_LOG_LINES).map(|index| format!("line {index}")));
        lines.push("thread 'b' panicked at src/b.rs:2:2:".to_owned());
        let log = log_file(&lines.join("\n"));
        let mut last_log_lines = VecDeque::new();

        assert_eq!(
            first_panic_in_log(&log, &mut last_log_lines)
                .expect("the test reads its log file")
                .as_deref(),
            Some("thread 'a' panicked at src/a.rs:1:1:")
        );
        assert_eq!(last_log_lines.len(), StopFailure::QUOTED_LOG_LINES);
        assert_eq!(
            last_log_lines.back().map(String::as_str),
            Some("thread 'b' panicked at src/b.rs:2:2:")
        );

        _ = fs::remove_file(log);
    }

    #[test]
    fn colour_codes_are_removed() {
        assert_eq!(
            without_colour("\u{1b}[2m2026\u{1b}[0m \u{1b}[34mDEBUG\u{1b}[0m ready"),
            "2026 DEBUG ready"
        );
    }

    fn failed_start_with_log(log: &str) -> FailedStart {
        FailedStart {
            command: "nisshi broker".to_owned(),
            exit_code: Some(1),
            log: log.to_owned(),
            reason: "exited with 1".to_owned(),
        }
    }

    #[test]
    fn error_lines_leave_out_the_storage_url_the_broker_prints_at_startup() {
        let failed_start = failed_start_with_log(
            "storage: sqlite:////proc/nisshi/nisshi.db [\"sqlite\"]\nError: Io(PermissionDenied)\n\
             storage: sqlite:////proc/nisshi/nisshi.db\n",
        );

        assert_eq!(
            failed_start.error_lines().collect::<Vec<_>>(),
            ["Error: Io(PermissionDenied)"]
        );
    }

    #[test]
    fn error_lines_include_an_argument_error_from_clap() {
        let failed_start = failed_start_with_log(
            "error: invalid value 'not a url' for '--storage-engine <STORAGE_ENGINE>'\n\nFor more \
             information, try '--help'.\n",
        );

        failed_start.assert_error_names("not a url");
    }

    #[test]
    fn error_lines_include_an_error_that_tracing_logged() {
        let failed_start = failed_start_with_log(
            "storage: postgres://localhost?maintenance_interval=0s [\"postgres\"]\n\
             2026-10-08T21:09:59.160193Z ERROR nisshi: 91: storage option maintenance_interval=0s \
             is not a valid interval\n\
             Error: Server(InvalidStorageOptionValue { option: \"maintenance_interval\" })\n",
        );

        failed_start.assert_error_names("maintenance_interval=0s");
    }

    #[test]
    fn error_lines_are_empty_when_the_broker_printed_no_error() {
        let failed_start =
            failed_start_with_log("thread 'main' panicked at src/main.rs:1:1:\nsome message\n");

        assert_eq!(failed_start.error_lines().count(), 0);
    }
}
