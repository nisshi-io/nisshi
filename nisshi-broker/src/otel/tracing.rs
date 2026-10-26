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

use crate::{Result, TracingFormat};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::{
    EnvFilter, filter::LevelFilter, fmt::format::FmtSpan, layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[derive(Debug)]
pub(super) struct Guard {
    tracer: Option<SdkTracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(tracer) = self.tracer.as_ref()
            && let Err(err) = tracer.shutdown()
        {
            eprintln!("{err:?}")
        }
    }
}

/// The env filter, built from the `RUST_LOG` environment variable.
fn env_filter() -> Result<EnvFilter> {
    env_filter_from(std::env::var("RUST_LOG").ok().as_deref())
}

/// The env filter for a `RUST_LOG` value, `None` when it is unset.
///
/// The level is `warn` when `rust_log` is unset or empty, so warnings such
/// as deprecation notices show on a default run; any directives in `rust_log`
/// replace that default. Malformed directives are ignored, as they always
/// have been.
///
/// `reqsign_core` is capped at `info` regardless of the ambient level.
/// reqsign-core 3.3.2 logs each credential provider it tries at `debug`, and
/// reqsign-aws-core 3.2.0's `StaticCredentialProvider` derives `Debug` over
/// `secret_access_key` and `session_token` without redaction, so
/// `RUST_LOG=debug` (routine when troubleshooting, and what CI's test job
/// uses) prints AWS secrets in cleartext through this subscriber's `log`
/// compatibility layer.
fn env_filter_from(rust_log: Option<&str>) -> Result<EnvFilter> {
    Ok(EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .parse_lossy(rust_log.unwrap_or_default())
        .add_directive("reqsign_core=info".parse()?))
}

pub(super) fn init_tracing_subscriber(tracing_format: TracingFormat) -> Result<Guard> {
    // let provider = init_tracer_provider()?;

    // let tracer = provider.tracer(format!("{}-otel-subscriber", env!("CARGO_PKG_NAME")));

    match tracing_format {
        TracingFormat::Text => tracing_subscriber::registry()
            .with(env_filter()?)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_line_number(true)
                    .with_thread_ids(false)
                    .with_span_events(FmtSpan::FULL),
            )
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),

        TracingFormat::Json => tracing_subscriber::registry()
            .with(env_filter()?)
            .with(tracing_subscriber::fmt::layer().json())
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),
    }

    Ok(Guard { tracer: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use tracing::{Level, enabled};

    /// Runs `check` under a thread-local subscriber filtered by `rust_log`, so
    /// the process environment is never read or changed.
    fn with_filter<T>(rust_log: Option<&str>, check: impl FnOnce() -> T) -> Result<T> {
        let subscriber = tracing_subscriber::registry().with(env_filter_from(rust_log)?);
        Ok(tracing::subscriber::with_default(subscriber, check))
    }

    #[test]
    fn unset_rust_log_enables_warn_but_not_info() -> Result<()> {
        let levels = with_filter(None, || (enabled!(Level::WARN), enabled!(Level::INFO)))?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn empty_rust_log_enables_warn_but_not_info() -> Result<()> {
        let levels = with_filter(Some(""), || (enabled!(Level::WARN), enabled!(Level::INFO)))?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn explicit_rust_log_replaces_the_default() -> Result<()> {
        let debug = with_filter(Some("debug"), || enabled!(Level::DEBUG))?;
        assert_matches!(debug, true);

        let levels = with_filter(Some("error"), || {
            (enabled!(Level::ERROR), enabled!(Level::WARN))
        })?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn malformed_rust_log_is_ignored() -> Result<()> {
        let levels = with_filter(Some("=nonsense=["), || {
            (enabled!(Level::WARN), enabled!(Level::INFO))
        })?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn reqsign_core_stays_capped_at_info_under_debug() -> Result<()> {
        let levels = with_filter(Some("debug"), || {
            (
                enabled!(target: "reqsign_core", Level::INFO),
                enabled!(target: "reqsign_core", Level::DEBUG),
                enabled!(target: "other_crate", Level::DEBUG),
            )
        })?;
        assert_matches!(levels, (true, false, true));
        Ok(())
    }
}
