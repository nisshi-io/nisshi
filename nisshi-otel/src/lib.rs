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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use opentelemetry::{KeyValue, Value, global};
use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig as _};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::debug;
use url::{ParseError, Url};

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    ExporterBuild(Arc<ExporterBuildError>),
    Parse(#[from] ParseError),
}

impl From<ExporterBuildError> for Error {
    fn from(value: ExporterBuildError) -> Self {
        Self::ExporterBuild(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Resource attached to exported metrics.
///
/// Attributes come from `OTEL_RESOURCE_ATTRIBUTES` (via the SDK's
/// `EnvResourceDetector`). `service.name` is, in order: `OTEL_SERVICE_NAME`,
/// `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, then `fallback_service_name`.
/// Unset or empty values fall through to the next source.
pub fn resource(fallback_service_name: impl Into<Value>) -> Resource {
    Resource::builder_empty()
        .with_service_name(fallback_service_name)
        .build()
}

pub fn meter_provider(
    otlp_endpoint_url: Url,
    service_name: impl Into<String>,
) -> Result<SdkMeterProvider> {
    otlp_endpoint_url
        .join("v1/metrics")
        .inspect(|endpoint| debug!(%endpoint))
        .map_err(Into::into)
        .and_then(|endpoint| {
            opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(endpoint.to_string())
                .build()
                .map_err(Into::into)
        })
        .map(|exporter| {
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(
                    Resource::builder_empty()
                        .with_attributes([KeyValue::new(SERVICE_NAME, service_name.into())])
                        .build(),
                )
                .build();

            global::set_meter_provider(meter_provider.clone());

            meter_provider
        })
}

#[cfg(test)]
mod tests {
    use opentelemetry::Key;
    use temp_env::{with_vars, with_vars_unset};

    use super::*;

    const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";
    const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";
    const FALLBACK: &str = "fallback-svc";

    fn service_name(resource: &Resource) -> Option<String> {
        resource
            .get(&Key::new(SERVICE_NAME))
            .map(|value| value.to_string())
    }

    fn attribute(resource: &Resource, key: &'static str) -> Option<String> {
        resource.get(&Key::new(key)).map(|value| value.to_string())
    }

    #[test]
    fn falls_back_to_package_name_when_env_unset() {
        with_vars_unset([OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES], || {
            let resource = resource(FALLBACK);
            assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
        });
    }

    #[test]
    fn otel_service_name_sets_service_name() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("svc-a")),
                (OTEL_RESOURCE_ATTRIBUTES, None),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-a".to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn resource_attributes_set_service_name_and_extra_attributes() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (
                    OTEL_RESOURCE_ATTRIBUTES,
                    Some("service.name=svc-b,service.version=1.2.3"),
                ),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-b".to_owned()), service_name(&resource));
                assert_eq!(
                    Some("1.2.3".to_owned()),
                    attribute(&resource, "service.version")
                );
            },
        );
    }

    #[test]
    fn otel_service_name_wins_over_resource_attributes() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("svc-a")),
                (
                    OTEL_RESOURCE_ATTRIBUTES,
                    Some("service.name=svc-b,deployment.environment.name=staging"),
                ),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-a".to_owned()), service_name(&resource));
                assert_eq!(
                    Some("staging".to_owned()),
                    attribute(&resource, "deployment.environment.name")
                );
                assert_eq!(
                    1,
                    resource
                        .iter()
                        .filter(|(key, _)| key.as_str() == SERVICE_NAME)
                        .count()
                );
            },
        );
    }

    #[test]
    fn empty_otel_service_name_is_treated_as_unset() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("")),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.name=svc-b")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-b".to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn resource_attributes_without_service_name_keep_fallback() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.version=9")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
                assert_eq!(
                    Some("9".to_owned()),
                    attribute(&resource, "service.version")
                );
            },
        );
    }

    #[test]
    fn empty_service_name_attribute_is_treated_as_unset() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.name=")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
            },
        );
    }
}
