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

//! Runs external commands with a time limit.
//!
//! The harness runs every Kafka CLI call through here, and every `docker` command that can block:
//! starting a container, which may pull an image, and stopping one. `std::process::Command::output`
//! waits for as long as a command runs, so a command that hangs would hold its test until nextest
//! kills it minutes later, without saying which command hung. These functions kill a command that
//! outlives its time limit and report what it printed.

use std::{
    io::{Read, Write as _},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

/// What a finished command printed.
#[derive(Debug)]
pub(crate) struct Finished {
    pub(crate) code: Option<i32>,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// Runs `command` to completion, feeding it `input`, and kills it after
/// `timeout`. A timeout is an error rather than a hang, so a stuck command
/// fails its test instead of holding the whole run.
pub(crate) fn run(
    command: &mut Command,
    input: Option<&str>,
    timeout: Duration,
) -> Result<Finished, String> {
    let mut child = command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("{command:?} could not start: {err}"))?;

    let stdout = drain(child.stdout.take());
    let stderr = drain(child.stderr.take());

    // Stdin is written from its own thread, so a command that stops reading
    // it still hits the timeout. A command that exits before reading all of
    // its input is judged by its exit code, not by the failed write.
    if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
        let input = input.to_owned();
        drop(thread::spawn(move || stdin.write_all(input.as_bytes())));
    }

    let status = wait(&mut child, timeout);

    let stdout = stdout.join().unwrap_or_default();
    let stderr = stderr.join().unwrap_or_default();

    match status {
        Some(status) => Ok(Finished {
            code: status.code(),
            stdout,
            stderr,
        }),

        None => Err(format!(
            "{command:?} did not finish within {timeout:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )),
    }
}

/// Waits up to `timeout` for `child` to exit, killing it if it doesn't.
pub(crate) fn wait(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),

            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),

            _ => {
                _ = child.kill();
                _ = child.wait();
                return None;
            }
        }
    }
}

fn drain(pipe: Option<impl Read + Send + 'static>) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut buffer = Vec::new();

        if let Some(mut pipe) = pipe {
            _ = pipe.read_to_end(&mut buffer);
        }

        String::from_utf8_lossy(&buffer).into_owned()
    })
}
