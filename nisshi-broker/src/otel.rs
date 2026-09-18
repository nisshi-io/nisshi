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

use ::tracing::debug;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use url::Url;

use crate::{Result, TracingFormat};

mod tracing;

#[derive(Debug)]
pub struct Guard {
    #[allow(dead_code)]
    tracer: tracing::Guard,
}

pub fn init(tracing_format: TracingFormat) -> Result<Guard> {
    tracing::init_tracing_subscriber(tracing_format).map(|tracer| Guard { tracer })
}

pub fn metric_exporter(endpoint: Url) -> Result<()> {
    let endpoint = endpoint
        .join("v1/metrics")
        .inspect(|endpoint| debug!(%endpoint))?;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint.to_string())
        .build()?;

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", env!("CARGO_PKG_NAME"))])
                .build(),
        )
        .build();

    global::set_meter_provider(meter_provider);

    Ok(())
}
