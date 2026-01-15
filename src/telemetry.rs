//! OpenTelemetry integration for Viator.
//!
//! This module provides distributed tracing and metrics export via OpenTelemetry.
//! Enable with `--features telemetry`.
//!
//! # Example
//!
//! ```bash
//! # Start Viator with telemetry enabled, exporting to an OTLP collector
//! OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 viator --port 6379
//! ```

use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::{runtime, Resource};
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize OpenTelemetry tracing.
///
/// This sets up:
/// - OTLP exporter for traces (configurable via `OTEL_EXPORTER_OTLP_ENDPOINT`)
/// - Integration with the `tracing` crate
/// - Service name and version metadata
///
/// # Errors
///
/// Returns an error if the OTLP exporter fails to initialize.
pub fn init_telemetry() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "viator"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter, runtime::Tokio)
        .build();

    let tracer = provider.tracer("viator");

    // Register the global tracer provider
    opentelemetry::global::set_tracer_provider(provider);

    // Create the OpenTelemetry tracing layer
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Set up the subscriber with both fmt and telemetry layers
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer)
        .init();

    Ok(())
}

/// Shutdown OpenTelemetry, flushing any pending traces.
pub fn shutdown_telemetry() {
    opentelemetry::global::shutdown_tracer_provider();
}

/// A guard that shuts down telemetry when dropped.
pub struct TelemetryGuard;

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        shutdown_telemetry();
    }
}

/// Initialize telemetry and return a guard that cleans up on drop.
///
/// # Errors
///
/// Returns an error if telemetry initialization fails.
pub fn init_telemetry_with_guard() -> Result<TelemetryGuard, Box<dyn std::error::Error + Send + Sync>> {
    init_telemetry()?;
    Ok(TelemetryGuard)
}
