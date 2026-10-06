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

//! `smoke-broker -- <command>...` starts the broker the smoke tests share,
//! runs the command with `NISSHI_SMOKE_BOOTSTRAP` pointing at it, then stops
//! the broker. The run fails if the command fails, or if the broker exited,
//! restarted, panicked or didn't exit with 0 on SIGTERM.
//!
//! nextest runs each test in its own process, so no test can own a broker
//! the others share; `just smoke` owns it through this instead.

use std::{path::PathBuf, process::Command, process::ExitCode};

use nisshi_smoke_test::{Broker, LaunchOptions, free_port};

fn main() -> ExitCode {
    let command = std::env::args()
        .skip(1)
        .skip_while(|arg| arg == "--")
        .collect::<Vec<_>>();

    let Some((program, args)) = command.split_first() else {
        eprintln!("usage: smoke-broker -- <command>...");
        return ExitCode::FAILURE;
    };

    let broker = Broker::launch(LaunchOptions {
        port: free_port(),
        cluster_id: "nisshi-smoke".to_owned(),
        args: Vec::new(),
        log: std::env::var_os("NISSHI_SMOKE_LOG").map(PathBuf::from),
    });

    let status = Command::new(program)
        .args(args)
        .env("NISSHI_SMOKE_BOOTSTRAP", broker.bootstrap())
        .status();

    let stopped = broker.stop();

    let mut code = ExitCode::SUCCESS;

    let suite = match status {
        Ok(status) if status.success() => "PASS",

        Ok(status) => {
            eprintln!("smoke-broker: {program} exited with {status}");
            code = ExitCode::FAILURE;
            "FAIL"
        }

        Err(err) => {
            eprintln!("smoke-broker: could not run {program}: {err}");
            code = ExitCode::FAILURE;
            "FAIL"
        }
    };

    // The report shows only the rows in the results file, and each test gets
    // one row. Some failures don't belong to any test, so smoke-broker adds
    // two rows of its own: `suite` fails when the nextest run fails, and
    // `shared_broker` fails when the shared broker fails its checks.
    eprintln!("smoke-broker: result suite,{suite}");

    let outcome = match stopped {
        Ok(()) => "PASS",

        Err(reason) => {
            eprintln!("smoke-broker: {reason}");
            code = ExitCode::FAILURE;
            "FAIL"
        }
    };

    eprintln!("smoke-broker: result shared_broker,{outcome}");

    code
}
