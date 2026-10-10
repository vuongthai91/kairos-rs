//! Observability layer for kairos-rs (PR8 — OpenTelemetry tracing).
//!
//! This module wraps the OpenTelemetry SDK with a kairos-friendly API:
//! - [`tracing::TelemetryConfig`] is a serializable config block loaded
//!   from `config.json` (see `config_advanced.json` for a fully
//!   commented example).
//! - [`tracing::init_telemetry`] installs the global tracer provider
//!   (with a `BatchSpanProcessor` exporting via OTLP/gRPC), the W3C
//!   Trace Context propagator, and the `tracing` → OTel bridge
//!   subscriber. It returns a [`tracing::TelemetryGuard`] whose
//!   `Drop` flushes pending spans and shuts the provider down
//!   gracefully.
//! - W3C Trace Context propagation helpers ([`tracing::HeaderExtractor`]
//!   and [`tracing::HeaderInjector`]) let us pull the `traceparent`
//!   header off the incoming actix request and inject it into the
//!   outgoing reqwest upstream call.
//! - [`tracing::current_trace_id`] / [`tracing::current_span_id`] are
//!   used by the HTTP layer to stamp `X-Trace-Id` / `X-Span-Id` onto
//!   the response so clients can correlate with backend logs.
//! - [`middleware::TracingMiddleware`] is an actix-web middleware
//!   that creates a root `http.request` span for every incoming
//!   request, propagates the upstream `traceparent` as the span's
//!   parent, and records the response status code. Register it as
//!   the outermost `.wrap(...)` so every other middleware falls
//!   inside the span.
//!
//! # When telemetry is disabled
//!
//! All the helpers above short-circuit to no-ops when
//! `init_telemetry` has not been called: `is_initialized()` returns
//! `false`, `extract_context_from_request` returns `None`, and
//! `stamp_response_headers` writes nothing. The gateway keeps
//! working with effectively zero overhead — the only cost is one
//! `tracing::info_span!` allocation per request in the middleware,
//! which the `tracing` crate drops on scope exit without ever
//! touching the OTel SDK.

pub mod middleware;
pub mod tracing;
