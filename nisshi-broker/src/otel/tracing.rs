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
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
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

pub(super) fn init_tracing_subscriber(tracing_format: TracingFormat) -> Result<Guard> {
    // let provider = init_tracer_provider()?;

    // let tracer = provider.tracer(format!("{}-otel-subscriber", env!("CARGO_PKG_NAME")));

    match tracing_format {
        TracingFormat::Text => tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
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
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),
    }

    Ok(Guard { tracer: None })
}
