//! Tracing / observability initialization.
//!
//! By default the process logs to stdout via `tracing_subscriber::fmt`. When
//! built with the `otel` feature **and** given an OTLP endpoint, spans are also
//! exported to an OpenTelemetry collector over OTLP/gRPC (e.g. Tempo, Jaeger,
//! or the OpenTelemetry Collector).
//!
//! ## Enabling
//! ```text
//! cargo build -p nexora-app --features otel
//! nexora --otlp-endpoint http://localhost:4317 --otel-service-name nexora
//! ```
//!
//! Without the feature the `--otlp-endpoint` flag is accepted but a warning is
//! emitted noting that OTLP export is not compiled in.

use tracing_subscriber::EnvFilter;

/// Options controlling tracing initialization.
pub struct TracingOptions<'a> {
    /// OTLP/gRPC collector endpoint (e.g. `http://localhost:4317`). When `None`,
    /// only stdout logging is configured.
    pub otlp_endpoint: Option<&'a str>,
    /// Service name reported to the collector (`service.name` resource attr).
    /// Only read when the `otel` feature is enabled.
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    pub service_name: &'a str,
}

/// Guard that keeps the tracer provider alive and flushes spans on drop.
///
/// Hold it for the lifetime of the process (e.g. bind it in `main`). Dropping it
/// shuts the provider down, flushing any buffered spans to the collector.
#[must_use = "dropping the guard immediately shuts down span export"]
pub struct TracingGuard {
    #[cfg(feature = "otel")]
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!("OTLP tracer shutdown error: {e}");
            }
        }
    }
}

fn default_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialize the global tracing subscriber.
///
/// Always installs a stdout logging layer. If the `otel` feature is enabled and
/// an OTLP endpoint is supplied, also installs an OpenTelemetry export layer.
pub fn init(opts: TracingOptions<'_>) -> TracingGuard {
    #[cfg(feature = "otel")]
    {
        if let Some(endpoint) = opts.otlp_endpoint {
            match init_with_otlp(endpoint, opts.service_name) {
                Ok(guard) => {
                    tracing::info!(
                        "   OTLP:   exporting spans to {endpoint} (service.name={})",
                        opts.service_name
                    );
                    return guard;
                }
                Err(e) => {
                    // Fall back to stdout-only so the server still starts.
                    init_fmt_only();
                    tracing::error!("   OTLP:   failed to initialize exporter: {e}. Falling back to stdout logging only.");
                    return TracingGuard { provider: None };
                }
            }
        }
        init_fmt_only();
        TracingGuard { provider: None }
    }

    #[cfg(not(feature = "otel"))]
    {
        init_fmt_only();
        if opts.otlp_endpoint.is_some() {
            tracing::warn!(
                "   OTLP:   --otlp-endpoint provided but the 'otel' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features otel"
            );
        }
        TracingGuard {}
    }
}

/// Install a stdout-only `fmt` subscriber. Idempotent-safe: ignores the error if
/// a global subscriber is already set (e.g. in tests).
fn init_fmt_only() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(default_filter())
        .try_init();
}

#[cfg(feature = "otel")]
fn init_with_otlp(
    endpoint: &str,
    service_name: &str,
) -> Result<TracingGuard, Box<dyn std::error::Error>> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
    use tracing_subscriber::prelude::*;

    // Build the OTLP/gRPC span exporter pointing at the collector.
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("nexora");

    // Register the provider globally so any code using the OpenTelemetry API
    // (not just tracing spans) targets the same pipeline.
    opentelemetry::global::set_tracer_provider(provider.clone());

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(default_filter())
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()?;

    Ok(TracingGuard {
        provider: Some(provider),
    })
}
