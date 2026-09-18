// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use dotenv::dotenv;
use nisshi_broker::{TracingFormat, otel};
use nisshi_cli::{Cli, Result};
use nisshi_sans_io::ErrorCode;
use tracing::{debug, error};

const CLIENT_ERROR_MESSAGE: &str = "A client error occurred. Possible causes:
  • No network connection
  • The server is down or unreachable
  • Incorrect hostname or port
  • Firewall or proxy blocking the connection
  • TLS/SSL certificate issues (if applicable)

Check your internet connection, verify the server address, and try again.";

#[tokio::main]
async fn main() -> Result<ErrorCode> {
    _ = dotenv().ok();

    let _guard = otel::init(TracingFormat::Text)?;

    Cli::main()
        .await
        .inspect(|error_code| match error_code {
            ErrorCode::None => debug!("{}", error_code),
            _ => error!("{}", error_code),
        })
        .inspect_err(|err| match err {
            nisshi_cli::Error::Cat(error) => match &**error {
                nisshi_cat::Error::Client(_) => error!("{}", CLIENT_ERROR_MESSAGE),
                _ => error!("Unknown error occurred during command: {}", error),
            },
            nisshi_cli::Error::Generate(error) => match error {
                nisshi_generator::Error::Client(_) => error!("{}", CLIENT_ERROR_MESSAGE),
                _ => error!("Unknown error occurred during command: {}", error),
            },
            nisshi_cli::Error::Perf(error) => match error {
                nisshi_perf::Error::Client(_) => error!("{}", CLIENT_ERROR_MESSAGE),
                _ => error!("Unknown error occurred during command: {}", error),
            },
            nisshi_cli::Error::Proxy(error) => match error {
                nisshi_proxy::Error::Client(_) => error!("{}", CLIENT_ERROR_MESSAGE),
                _ => error!("Unknown error occurred during command: {}", error),
            },
            nisshi_cli::Error::Topic(error) => match error {
                nisshi_topic::Error::Client(_) => error!("{}", CLIENT_ERROR_MESSAGE),
                _ => error!("Unknown error occurred during command: {}", error),
            },
            _ => error!("Unknown error occurred during command: {}", err),
        })
}
