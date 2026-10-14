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

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use super::{STOP_TIMEOUT, Shutdown};
use crate::{
    broker::{LaunchOptions, files::BrokerFiles},
    common::{docker, timed_command, unique},
};

/// How long `docker run` has to start a broker container. `run.sh` pulls the image before the
/// tests run, so this covers only creating and starting the container, and stays well under the
/// time nextest gives a test before it kills it.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(60);

/// What `docker cp` prints when the file to copy isn't in the container.
const DOCKER_CP_MISSING_FILE: &str = "Could not find the file";

/// A broker that runs in a container from the Docker image in `NISSHI_SMOKE_IMAGE`.
#[derive(Debug)]
pub(in crate::broker) struct BrokerContainer {
    name: String,
    /// The broker's own directory on this machine, where the harness copies a file out of the
    /// container to read it.
    dir: PathBuf,
}

impl BrokerContainer {
    /// Starts `image` in a container, with a SQLite broker's volume in `files` as its working
    /// directory. Returns an error if the container can't start.
    pub(super) fn launch(
        image: &str,
        options: &LaunchOptions,
        files: &BrokerFiles,
    ) -> Result<Self, String> {
        let name = unique::name("nisshi-smoke-broker");

        let mut command = Command::new("docker");

        _ = command.args([
            "run",
            "--detach",
            "--name",
            &name,
            &docker::label(),
            "--network=host",
        ]);

        // The image sets its own PATH and HOME.
        for (variable, _) in super::broker_environment() {
            if variable != "PATH" && variable != "HOME" {
                _ = command.args(["--env", &variable]);
            }
        }

        if let Some(volume) = files.volume() {
            _ = command.args(["--volume", &format!("{volume}:/data"), "--workdir=/data"]);
        }

        _ = command.arg(image).args(options.broker_args());

        let started = timed_command::run(&mut command, None, CONTAINER_START_TIMEOUT);

        if !matches!(&started, Ok(started) if started.code == Some(0)) {
            remove_container(&name);
            return Err(format!("could not start {image}: {started:?}"));
        }

        Ok(Self {
            name,
            dir: files.dir().to_owned(),
        })
    }

    /// An error if the broker has exited.
    pub(super) fn check_running(&self) -> Result<(), String> {
        match docker::inspect(&self.name, "{{.State.Running}} {{.State.ExitCode}}")?.split_once(' ')
        {
            Some(("true", _)) => Ok(()),
            Some((_, code)) => Err(format!("exited with {code}")),
            None => Err(format!("container {} has no state", self.name)),
        }
    }

    /// Stops the container as a broker process is stopped, saves what it printed to `log`, and
    /// removes it. Adds each check that it fails to `failures`.
    pub(super) fn stop(self, shutdown: Shutdown, log: &Path, failures: &mut Vec<String>) {
        if let Err(reason) = self.check_running() {
            failures.push(format!("broker {reason} during the run"));
        } else {
            self.signal_and_check_exit(shutdown, failures);
        }

        failures.extend(self.save_logs(log).err());
        remove_container(&self.name);
    }

    fn signal_and_check_exit(&self, shutdown: Shutdown, failures: &mut Vec<String>) {
        let name = &self.name;

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

                match docker::inspect(name, "{{.State.ExitCode}}") {
                    Ok(code) if code == "0" => {}
                    Ok(code) => failures.push(format!("broker exited with {code} on SIGTERM")),
                    Err(reason) => failures.push(reason),
                }
            }

            Shutdown::Kill => match docker::execute(&["kill", "--signal=KILL", name]) {
                Ok(killed) if killed.code == Some(0) => {}
                killed => failures.push(format!("couldn't kill {name}: {killed:?}")),
            },
        }
    }

    /// Kills a broker container that didn't start, if it is still running, saves what it printed
    /// to `log`, and removes it. Returns its exit code, or `None` if the harness killed it.
    pub(super) fn stop_after_failed_start(&self, log: &Path) -> Option<i32> {
        let code = match self.check_running() {
            Ok(()) => {
                _ = docker::execute(&["kill", &self.name]);
                None
            }

            Err(_) => docker::inspect(&self.name, "{{.State.ExitCode}}")
                .ok()
                .and_then(|code| code.parse().ok()),
        };

        // Save the container's output before removing the container deletes it.
        // `FailedStart` reads the broker's error from this log, so without it a test
        // that expects a refusal fails.
        _ = self.save_logs(log);
        remove_container(&self.name);
        code
    }

    #[track_caller]
    pub(super) fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        let name = &self.name;

        // The broker's image holds only the broker, without a shell or `stat`, so the
        // harness copies the file out with `docker cp`, which keeps its modification time.
        // `docker cp` reads a relative path from the container's root, not from its
        // working directory.
        let working_dir = match docker::inspect(name, "{{.Config.WorkingDir}}") {
            Ok(working_dir) => working_dir,
            Err(reason) => panic!("{reason}"),
        };
        let path_in_container = Path::new("/").join(working_dir).join(path);

        let copy = self.dir.join(unique::name("copied"));
        let source = format!("{name}:{}", path_in_container.display());

        let copied = match docker::execute(&["cp", &source, &copy.display().to_string()]) {
            Ok(copied) => copied,
            Err(reason) => panic!("{reason}"),
        };

        let metadata = match copied.code {
            Some(0) => match fs::metadata(&copy) {
                Ok(metadata) => Some(metadata),
                Err(err) => panic!("{}: {err}", copy.display()),
            },
            _ if copied.stderr.contains(DOCKER_CP_MISSING_FILE) => None,
            _ => panic!("docker cp {source}: {}", copied.stderr.trim()),
        };

        _ = fs::remove_file(&copy);
        metadata
    }

    /// Writes what the container printed to `log`.
    fn save_logs(&self, log: &Path) -> Result<(), String> {
        let logs = docker::execute(&["logs", &self.name])?;

        if logs.code != Some(0) {
            return Err(format!("docker logs {}: {}", self.name, logs.stderr.trim()));
        }

        fs::write(log, logs.stdout + logs.stderr.as_str())
            .map_err(|err| format!("couldn't write {}: {err}", log.display()))
    }
}

impl fmt::Display for BrokerContainer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "container {}", self.name)
    }
}

/// Removes a container that the harness started, but not a broker's volume, which
/// [`BrokerFiles`] removes.
fn remove_container(name: &str) {
    _ = docker::execute(&["rm", "--force", "--volumes", name]);
}
