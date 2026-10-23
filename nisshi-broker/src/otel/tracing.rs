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
use tracing::Subscriber;
use tracing_subscriber::{
    EnvFilter, Layer as _,
    filter::FilterExt as _,
    fmt::format::FmtSpan,
    layer::{Filter, SubscriberExt},
    registry::LookupSpan,
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

/// The targets that the broker never logs below `info`, as an env filter that allows every other
/// target at every level.
const CAPS: &str = "trace,reqsign_core=info,turso_core=info";

/// `ambient` with `reqsign_core` and `turso_core` capped at `info`: an event passes when `ambient`
/// enables it and the caps do too, so a crate is never more verbose than the lower of its level in
/// `ambient` and `info`.
///
/// reqsign-core 3.3.2 logs each credential provider it tries at `debug`, and
/// reqsign-aws-core 3.2.0's `StaticCredentialProvider` derives `Debug` over
/// `secret_access_key` and `session_token` without redaction, so
/// `RUST_LOG=debug` (routine when troubleshooting, and what CI's test job
/// uses) prints AWS secrets in cleartext through this subscriber's `log`
/// compatibility layer.
///
/// turso_core 0.8.3 logs the B-tree cell that it deletes or balances at
/// `debug`, with the cell's payload, which is the stored record's key and
/// value, so `RUST_LOG=debug` on a `turso://` broker writes every record that
/// the broker deletes to the log.
///
/// A directive such as `turso_core=info` in `ambient` would turn those crates' `info` events on
/// when `ambient` is `warn` or, with `RUST_LOG` unset, `error`, and the checkpoint of Turso's MVCC
/// log alone logged 17,000 to 22,000 `info` lines in a 4 minute benchmark run. The caps are a
/// second filter, and a layer applies both.
pub fn capped_filter<S>(ambient: EnvFilter) -> Result<impl Filter<S>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    Ok(ambient.and(EnvFilter::try_new(CAPS)?))
}

pub(super) fn init_tracing_subscriber(tracing_format: TracingFormat) -> Result<Guard> {
    // let provider = init_tracer_provider()?;

    // let tracer = provider.tracer(format!("{}-otel-subscriber", env!("CARGO_PKG_NAME")));

    match tracing_format {
        TracingFormat::Text => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_line_number(true)
                    .with_thread_ids(false)
                    .with_span_events(FmtSpan::FULL)
                    .with_filter(capped_filter(EnvFilter::from_default_env())?),
            )
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),

        TracingFormat::Json => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_filter(capped_filter(EnvFilter::from_default_env())?),
            )
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),
    }

    Ok(Guard { tracer: None })
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use super::*;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|err| io::Error::other(err.to_string()))?
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    const MARKERS: [&str; 7] = [
        "TURSO-DEBUG-PAYLOAD",
        "TURSO-INFO",
        "TURSO-WARN",
        "REQSIGN-DEBUG-SECRET",
        "REQSIGN-INFO",
        "OTHER-DEBUG",
        "OTHER-ERROR",
    ];

    /// The same events every time, so that each runs under every filter of a test.
    fn emit() {
        tracing::debug!(target: "turso_core::storage::btree", "TURSO-DEBUG-PAYLOAD");
        tracing::info!(target: "turso_core::mvcc::database", "TURSO-INFO");
        tracing::warn!(target: "turso_core::mvcc::database", "TURSO-WARN");
        tracing::debug!(target: "reqsign_core::provider", "REQSIGN-DEBUG-SECRET");
        tracing::info!(target: "reqsign_core::provider", "REQSIGN-INFO");
        tracing::debug!(target: "nisshi_broker::other", "OTHER-DEBUG");
        tracing::error!(target: "nisshi_broker::other", "OTHER-ERROR");
    }

    /// The markers of the events that the broker's filter lets through when `ambient` is the
    /// value of `RUST_LOG`.
    fn shown(ambient: &str) -> Result<Vec<&'static str>> {
        let captured = Captured::default();
        let writer = captured.clone();

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(capped_filter(EnvFilter::new(ambient))?),
        );

        tracing::subscriber::with_default(subscriber, emit);

        let output = String::from_utf8_lossy(
            &captured
                .0
                .lock()
                .map_err(|err| crate::Error::Message(err.to_string()))?,
        )
        .into_owned();

        Ok(MARKERS
            .into_iter()
            .filter(|marker| output.contains(marker))
            .collect())
    }

    /// A crate that is capped is never more verbose than the lower of its level in the ambient
    /// filter and `info`. A directive that sets it to `info` would turn its `info` events on under
    /// `RUST_LOG=warn`, and under the default of `error`.
    #[test]
    fn a_capped_crate_is_never_more_verbose_than_ambient_or_info() -> Result<()> {
        for (ambient, expected) in [
            // more verbose than info: debug is capped, and the other crates keep theirs
            (
                "trace",
                vec![
                    "TURSO-INFO",
                    "TURSO-WARN",
                    "REQSIGN-INFO",
                    "OTHER-DEBUG",
                    "OTHER-ERROR",
                ],
            ),
            (
                "debug",
                vec![
                    "TURSO-INFO",
                    "TURSO-WARN",
                    "REQSIGN-INFO",
                    "OTHER-DEBUG",
                    "OTHER-ERROR",
                ],
            ),
            // less verbose than info: not raised to it
            ("warn", vec!["TURSO-WARN", "OTHER-ERROR"]),
            ("error", vec!["OTHER-ERROR"]),
            ("off", vec![]),
            // a level for one crate, over another for the rest
            (
                "warn,nisshi_broker=debug",
                vec!["TURSO-WARN", "OTHER-DEBUG", "OTHER-ERROR"],
            ),
            (
                "turso_core=warn,info",
                vec!["TURSO-WARN", "REQSIGN-INFO", "OTHER-ERROR"],
            ),
            // asking for `debug` does not get it
            (
                "warn,turso_core=debug,reqsign_core=trace",
                vec!["TURSO-INFO", "TURSO-WARN", "REQSIGN-INFO", "OTHER-ERROR"],
            ),
        ] {
            assert_eq!(expected, shown(ambient)?, "RUST_LOG={ambient}");
        }

        Ok(())
    }

    /// `RUST_LOG` unset gives the filter that `EnvFilter::from_default_env` builds, which shows
    /// `error` events and nothing below them, and the caps must not add anything to that.
    #[test]
    fn nothing_is_added_to_the_default_filter() -> Result<()> {
        let default_filter = EnvFilter::builder()
            .with_default_directive(tracing::level_filters::LevelFilter::ERROR.into())
            .parse("")?;

        let captured = Captured::default();
        let writer = captured.clone();

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(capped_filter(default_filter)?),
        );

        tracing::subscriber::with_default(subscriber, emit);

        let output = String::from_utf8_lossy(
            &captured
                .0
                .lock()
                .map_err(|err| crate::Error::Message(err.to_string()))?,
        )
        .into_owned();

        assert!(output.contains("OTHER-ERROR"), "{output}");
        for marker in MARKERS
            .into_iter()
            .filter(|marker| *marker != "OTHER-ERROR")
        {
            assert!(!output.contains(marker), "{marker} in {output}");
        }

        Ok(())
    }
}
