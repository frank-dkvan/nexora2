//! OpenTelemetry tracing setup for distributed tracing

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Config, TracerProvider};
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Initialize OpenTelemetry tracing with OTLP exporter
pub fn init_tracing(service_name: &str, otlp_endpoint: Option<&str>) -> anyhow::Result<()> {
    let resource = Resource::new(vec![KeyValue::new(
        "service.name",
        service_name.to_string(),
    )]);

    let tracer_provider = if let Some(endpoint) = otlp_endpoint {
        // Export to OTLP collector (e.g., Jaeger, Tempo)
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .build_span_exporter()?;

        TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_config(Config::default().with_resource(resource))
            .build()
    } else {
        // No exporter - traces stay local (for testing)
        TracerProvider::builder()
            .with_config(Config::default().with_resource(resource))
            .build()
    };

    let tracer = tracer_provider.tracer(service_name.to_string());

    // Setup tracing subscriber with OpenTelemetry layer
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    Registry::default()
        .with(env_filter)
        .with(telemetry_layer)
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    Ok(())
}

/// Shutdown tracing and flush remaining spans
pub async fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_tracing_no_exporter() {
        // Should succeed without OTLP endpoint
        let result = init_tracing("nexora-test", None);
        assert!(result.is_ok());
    }
}
