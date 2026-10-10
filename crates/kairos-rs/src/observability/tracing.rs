//! OpenTelemetry distributed tracing — config, W3C propagation,
//! guard. See [`crate::observability`] for the module-level overview.
//!
//! Why a guard instead of `OnceCell`? The guard's `Drop` runs the
//! SDK's batch exporter's flush on shutdown, ensuring any in-flight
//! spans get exported before the process exits. Without it, the last
//! few spans of a graceful shutdown would silently disappear.

use log::warn;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{TraceContextExt, TraceId, TracerProvider as TraceProviderTrait};
use opentelemetry::{global, Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchSpanProcessor, Config as TraceConfig, Sampler, TracerProvider as SdkTracerProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

/// Serializable telemetry config. Loaded from `config.json` under a
/// top-level `telemetry` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Master switch. When `false`, `init_telemetry` is a no-op and
    /// `current_trace_id` always returns `None`. Default: `false`.
    #[serde(default)]
    pub enabled: bool,
    /// `service.name` resource attribute. Stamped on every span so
    /// backends (Tempo, Jaeger, Honeycomb) can group by service.
    /// Default: `"kairos-gateway"`.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// OTLP/gRPC endpoint, e.g. `http://otel-collector:4317`. When
    /// `None`, `init_telemetry` is a no-op (no exporter installed).
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Head-based sampler ratio in `[0.0, 1.0]`. `1.0` samples every
    /// request, `0.0` drops everything. The default `1.0` is
    /// appropriate for development; production deployments should
    /// tune this down (e.g. `0.1` for high-RPS services).
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
}

fn default_service_name() -> String {
    "kairos-gateway".to_string()
}

fn default_sample_ratio() -> f64 {
    1.0
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: default_service_name(),
            otlp_endpoint: None,
            sample_ratio: default_sample_ratio(),
        }
    }
}

impl TelemetryConfig {
    /// Reject obviously-broken configs early so `init_telemetry`
    /// doesn't have to. Returns `Err(msg)` on the first problem.
    pub fn validate(&self) -> Result<(), String> {
        if !self.sample_ratio.is_finite() || !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(format!(
                "telemetry.sample_ratio must be in [0.0, 1.0], got {}",
                self.sample_ratio
            ));
        }
        if self.service_name.trim().is_empty() {
            return Err("telemetry.service_name must not be empty".to_string());
        }
        if self.enabled && self.otlp_endpoint.is_none() {
            return Err(
                "telemetry.enabled = true requires telemetry.otlp_endpoint to be set"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Process-wide flag flipped to `true` after a successful
/// `init_telemetry`. The HTTP layer uses this to short-circuit
/// `current_trace_id` lookups when tracing is disabled (avoids the
/// `Span::current()` global lookup cost on every request).
static TELEMETRY_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Install the global OpenTelemetry tracer provider. Returns
/// `Some(guard)` on success; the guard's `Drop` flushes pending
/// spans on shutdown. Returns `None` when the config is disabled
/// or the OTLP endpoint is unset (the common case for local dev).
///
/// # Performance
///
/// Each call constructs a fresh `TracerProvider`, so this is intended
/// to be called exactly once during process startup (from
/// `crates/kairos-gateway/src/main.rs`). Calling it more than once
/// will leak the previous provider.
pub fn init_telemetry(config: &TelemetryConfig) -> Option<TelemetryGuard> {
    if !config.enabled {
        return None;
    }
    let endpoint = config.otlp_endpoint.as_deref()?;
    if endpoint.trim().is_empty() {
        return None;
    }
    // Build the OTLP/gRPC span exporter. `build_span_exporter`
    // returns an error on a malformed endpoint (e.g. unparsable
    // URI). We treat that as "tracing disabled" rather than crash
    // — the gateway stays up, operators get a warning in the log.
    let exporter = match opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint)
        .build_span_exporter()
    {
        Ok(exporter) => exporter,
        Err(err) => {
            warn!(
                "telemetry: failed to build OTLP span exporter for endpoint {}: {}",
                endpoint, err
            );
            return None;
        }
    };

    // Resource: only `service.name` for now. Operators who need
    // extra resource attributes can layer them in via
    // `OTEL_RESOURCE_ATTRIBUTES` env var (the SDK respects it).
    let resource = Resource::new(vec![KeyValue::new(
        "service.name",
        config.service_name.clone(),
    )]);

    // Sampler: ParentBased(TraceIdRatioBased) so we always honor
    // the upstream sampling decision when a parent trace is
    // present, and apply our own ratio when the request starts a
    // new trace.
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        config.sample_ratio,
    )));

    // Build the SDK tracer provider with a batch span processor
    // that exports to the OTLP endpoint via Tokio. In
    // opentelemetry_sdk 0.24 the resource + sampler are wrapped
    // inside a `Config` (the builder only takes the exporter
    // directly).
    // Cap the per-export timeout so a misconfigured collector can't
    // stall gateway shutdown. The default 30s is way too long for a
    // service that may be restarting in a tight loop. We also force
    // `max_concurrent_exports = 0` so the BSP performs exports
    // synchronously on its own task — without this, the BSP spawns
    // an export task on the tokio runtime and `provider.shutdown()`
    // blocks on its `JoinHandle` when the test runtime is dropping.
    let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_max_export_timeout(std::time::Duration::from_millis(100))
        .with_max_concurrent_exports(0)
        .build();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(
            BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_batch_config(batch_config)
                .build(),
        )
        .with_config(TraceConfig::default().with_resource(resource).with_sampler(sampler))
        .build();

    // Install W3C Trace Context propagator so the W3C helpers
    // (`extract_context_from_request`, `inject_context_into_*`)
    // know how to serialize/deserialize `traceparent`.
    global::set_text_map_propagator(TraceContextPropagator::new());
    // Register the provider globally. The HTTP middleware /
    // RouteHandler will obtain tracers on demand via
    // `global::tracer("...")`.
    global::set_tracer_provider(provider.clone());

    // Bridge `tracing` -> OTel: install a global subscriber that
    // has both the OTel layer (export tracing spans/events to the
    // OTLP endpoint) and a fmt layer (so they also show up on
    // stdout in dev). `env_logger` (configured in
    // `configure_logger`) keeps handling `log` macros — the two
    // subscribers don't conflict because they listen to different
    // crates.
    //
    // `provider.tracer(...)` (not `global::tracer(...)`) because
    // the bridge needs an `SdkTracer` impl `PreSampledTracer` —
    // `BoxedTracer` from the global getter does not.
    let tracer = provider.tracer("kairos-gateway");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
    if let Err(err) = tracing::subscriber::set_global_default(
        Registry::default().with(otel_layer).with(fmt_layer),
    ) {
        // A subscriber is already set (e.g. by a test harness or
        // a hot-reload call). OTel still works via the global
        // provider — only the tracing->OTel bridge is missing.
        warn!(
            "telemetry: failed to install tracing-opentelemetry bridge: {}",
            err
        );
    }

    TELEMETRY_INITIALIZED.store(true, Ordering::Release);
    Some(TelemetryGuard {
        provider: Some(provider),
    })
}

/// Opaque handle. `Drop` flushes + shuts down the provider.
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // `shutdown` flushes pending spans. If the provider was
        // never used (no spans created), this is a no-op.
        //
        // We bound the wait with a hard wall-clock timeout so a
        // misconfigured collector (or a test runtime that's already
        // shutting down) can't stall the gateway indefinitely. The
        // BatchSpanProcessor's `max_export_timeout` (see
        // `init_telemetry`) caps the per-export wait, so the only
        // way we'd hit the outer timeout is if the BSP worker
        // thread is wedged — in which case dropping the in-flight
        // span is the right call anyway.
        if let Some(provider) = self.provider.take() {
            use std::sync::mpsc;
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = provider.shutdown();
                let _ = tx.send(());
            });
            let _ = rx.recv_timeout(std::time::Duration::from_secs(2));
        }
        TELEMETRY_INITIALIZED.store(false, Ordering::Release);
    }
}

/// `true` when `init_telemetry` was called successfully and the
/// global provider is live.
pub fn is_initialized() -> bool {
    TELEMETRY_INITIALIZED.load(Ordering::Acquire)
}

/// Current trace ID as a 32-char hex string, or `None` when tracing
/// is disabled or no span is active. Used to stamp `X-Trace-Id` onto
/// responses.
pub fn current_trace_id() -> Option<String> {
    if !is_initialized() {
        return None;
    }
    let trace_id = Context::current().span().span_context().trace_id();
    if trace_id == TraceId::INVALID {
        None
    } else {
        Some(trace_id.to_string())
    }
}

/// Current span ID as a 16-char hex string, or `None` when no span
/// is active. Used to stamp `X-Span-Id` onto responses.
pub fn current_span_id() -> Option<String> {
    if !is_initialized() {
        return None;
    }
    let span_id = Context::current().span().span_context().span_id();
    if span_id == opentelemetry::trace::SpanId::INVALID {
        None
    } else {
        Some(span_id.to_string())
    }
}

// ---------- W3C Trace Context propagation ----------

/// Adapter that lets OpenTelemetry's [`Extractor`] read from an
/// actix-web [`HeaderMap`](actix_web::http::header::HeaderMap).
/// Use with `global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(headers)))`.
pub struct HeaderExtractor<'a>(pub &'a actix_web::http::header::HeaderMap);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Adapter that lets OpenTelemetry's [`Injector`] write to an
/// actix-web [`HeaderMap`](actix_web::http::header::HeaderMap).
/// Use with `global::get_text_map_propagator(|prop| prop.inject_context(&cx, &mut HeaderInjector(headers)))`.
pub struct HeaderInjector<'a>(pub &'a mut actix_web::http::header::HeaderMap);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        use std::str::FromStr;
        if let (Ok(name), Ok(val)) = (
            actix_web::http::header::HeaderName::from_str(key),
            actix_web::http::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, val);
        }
    }
}

/// Convenience: stamp `X-Trace-Id` and `X-Span-Id` onto an actix
/// `HeaderMap` from the current span context. No-op when tracing is
/// disabled. Callers should invoke this just before returning the
/// `HttpResponse` so clients can correlate.
pub fn stamp_response_headers(headers: &mut actix_web::http::header::HeaderMap) {
    use actix_web::http::header::{HeaderName, HeaderValue};
    use std::str::FromStr;
    if let Some(trace_id) = current_trace_id() {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_str("x-trace-id"),
            HeaderValue::from_str(&trace_id),
        ) {
            headers.insert(name, val);
        }
    }
    if let Some(span_id) = current_span_id() {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_str("x-span-id"),
            HeaderValue::from_str(&span_id),
        ) {
            headers.insert(name, val);
        }
    }
    let _ = KeyValue::new("placeholder", "used-by-stamp");
}

/// Extract a W3C Trace Context from the incoming request headers.
/// Returns `None` when no `traceparent` header is present, or when
/// tracing is disabled. Callers should use the returned `Context`
/// to set up the current span before any work happens.
pub fn extract_context_from_request(
    headers: &actix_web::http::header::HeaderMap,
) -> Option<Context> {
    if !is_initialized() {
        return None;
    }
    let cx = global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(headers)));
    // `extract` returns the current context with the extracted
    // span/trace IDs merged in. If no `traceparent` header was
    // present, the context is the empty one.
    let span = cx.span();
    if span.span_context().trace_id() == TraceId::INVALID {
        None
    } else {
        Some(cx)
    }
}

/// Inject the current W3C Trace Context into an outgoing request's
/// headers, so the upstream service can continue the same trace.
/// No-op when tracing is disabled.
pub fn inject_context_into_request(
    cx: &Context,
    headers: &mut actix_web::http::header::HeaderMap,
) {
    if !is_initialized() {
        return;
    }
    let mut injector = HeaderInjector(headers);
    global::get_text_map_propagator(|prop| prop.inject_context(cx, &mut injector));
}

/// Adapter that lets OpenTelemetry's [`Injector`] write to a
/// `reqwest::header::HeaderMap` (the type the reqwest upstream
/// client uses). Used with `inject_context_into_reqwest_headers`.
struct ReqwestHeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl<'a> Injector for ReqwestHeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, val);
        }
    }
}

/// Inject the W3C Trace Context into a `reqwest::header::HeaderMap`
/// (the upstream client's header type). Companion to
/// [`inject_context_into_request`], which targets actix's
/// `HeaderMap`. No-op when tracing is disabled.
pub fn inject_context_into_reqwest_headers(
    cx: &Context,
    headers: &mut reqwest::header::HeaderMap,
) {
    if !is_initialized() {
        return;
    }
    let mut injector = ReqwestHeaderInjector(headers);
    global::get_text_map_propagator(|prop| prop.inject_context(cx, &mut injector));
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
    use std::str::FromStr;

    #[test]
    fn config_default_is_disabled() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.otlp_endpoint.is_none());
        assert_eq!(cfg.service_name, "kairos-gateway");
        assert!((cfg.sample_ratio - 1.0).abs() < 1e-9);
    }

    #[test]
    fn config_validate_rejects_bad_sample_ratio() {
        let mut cfg = TelemetryConfig::default();
        cfg.sample_ratio = 1.5;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = -0.1;
        assert!(cfg.validate().is_err());
        cfg.sample_ratio = f64::NAN;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_validate_rejects_empty_service_name() {
        let mut cfg = TelemetryConfig::default();
        cfg.service_name = "   ".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_validate_requires_endpoint_when_enabled() {
        let mut cfg = TelemetryConfig::default();
        cfg.enabled = true;
        assert!(cfg.validate().is_err(), "enabled without endpoint must fail");
        cfg.otlp_endpoint = Some("http://collector:4317".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn config_validate_accepts_valid_config() {
        let cfg = TelemetryConfig {
            enabled: true,
            service_name: "kairos".to_string(),
            otlp_endpoint: Some("http://collector:4317".to_string()),
            sample_ratio: 0.5,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn init_telemetry_returns_none_when_disabled() {
        let cfg = TelemetryConfig::default();
        assert!(init_telemetry(&cfg).is_none());
    }

    #[test]
    fn init_telemetry_returns_none_when_endpoint_unset() {
        let cfg = TelemetryConfig {
            enabled: true,
            service_name: "kairos".to_string(),
            otlp_endpoint: None,
            sample_ratio: 1.0,
        };
        assert!(init_telemetry(&cfg).is_none());
    }

    #[test]
    fn current_trace_id_is_none_when_disabled() {
        // No init_telemetry call, so the global provider isn't live.
        assert!(!is_initialized());
        assert!(current_trace_id().is_none());
        assert!(current_span_id().is_none());
    }

    #[test]
    fn header_extractor_reads_from_header_map() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("traceparent").unwrap(),
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        let ext = HeaderExtractor(&headers);
        assert_eq!(
            ext.get("traceparent"),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        let keys = ext.keys();
        assert!(keys.contains(&"traceparent"));
    }

    #[test]
    fn header_injector_writes_to_header_map() {
        let mut headers = HeaderMap::new();
        {
            let mut inj = HeaderInjector(&mut headers);
            inj.set("traceparent", "00-aaaa-bbbb-01".to_string());
        }
        assert_eq!(
            headers
                .get("traceparent")
                .and_then(|v| v.to_str().ok()),
            Some("00-aaaa-bbbb-01")
        );
    }

    #[test]
    fn stamp_response_headers_is_noop_when_disabled() {
        let mut headers = HeaderMap::new();
        stamp_response_headers(&mut headers);
        // No x-trace-id / x-span-id when tracing is disabled.
        assert!(headers.get("x-trace-id").is_none());
        assert!(headers.get("x-span-id").is_none());
    }

    // PR8: lifecycle test for `init_telemetry`. Combined into a
    // single test because each call sets a process-global tracer
    // provider — splitting into two would race when the test
    // harness runs threads in parallel. The guard's `Drop`
    // restores the "not initialized" state at the end of each
    // sub-scenario, so the test is order-independent.
    //
    // Uses `#[tokio::test]` (not `#[test]`) because the OTLP
    // tonic exporter needs a tokio runtime when constructing the
    // gRPC channel — calling it from a plain `#[test]` panics in
    // `hyper-util/src/rt/tokio.rs` with "there is no reactor
    // running".
    #[tokio::test]
    async fn init_telemetry_lifecycle() {
        // Scenario 1: a syntactically-valid endpoint returns
        // `Some(guard)`. The actual OTLP connection happens on
        // export, not on `build_span_exporter`, so we don't need
        // a real collector to be running.
        {
            let cfg = TelemetryConfig {
                enabled: true,
                service_name: "kairos-test-valid".to_string(),
                otlp_endpoint: Some("http://127.0.0.1:4317".to_string()),
                sample_ratio: 1.0,
            };
            let guard = init_telemetry(&cfg);
            assert!(
                guard.is_some(),
                "valid endpoint should return Some(TelemetryGuard)"
            );
            assert!(is_initialized());
            drop(guard);
            // After Drop, the global flag must reset.
            assert!(!is_initialized());
        }

        // Scenario 2: an unreachable endpoint must NOT panic. The
        // OTLP exporter builder succeeds (it doesn't connect at
        // build time), so the guard is still returned; the actual
        // connection failure happens asynchronously on export.
        // The key invariant here is "no panic, no crash".
        {
            let cfg = TelemetryConfig {
                enabled: true,
                service_name: "kairos-test-unreachable".to_string(),
                // Port 1 is reserved (tcpmux) and effectively never
                // bound by a kairos deployment, so the export
                // stream will fail to connect.
                otlp_endpoint: Some("http://127.0.0.1:1".to_string()),
                sample_ratio: 1.0,
            };
            let guard = init_telemetry(&cfg);
            // We don't assert `is_some` vs `is_none` here because
            // either is acceptable — what matters is the call
            // returns cleanly. The current implementation returns
            // `Some(_)` because `build_span_exporter` only checks
            // the URI format, not reachability.
            drop(guard);
            assert!(!is_initialized());
        }
    }
}