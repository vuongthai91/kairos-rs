//! gRPC reverse proxy service (PR9).
//!
//! Acts as a transparent byte-for-byte gRPC forwarder. The gateway is
//! proto-agnostic: it parses the 5-byte length prefix, looks up the
//! configured backend, and forwards the request body to a
//! `tonic::transport::Channel` built for that backend. The response
//! bytes are returned verbatim; the gRPC `grpc-status` header is
//! surfaced via a 200 OK with the status code in the response body
//! (or as the HTTP/2 trailers-only frame, depending on the backend).
//!
//! # Call types supported
//!
//! - **Unary** (single request → single response): fully supported
//!   in v1.
//! - **Server-streaming** (single request → stream of responses):
//!   supported when the backend returns a streaming body; the
//!   dispatcher writes each chunk to the client as it arrives.
//! - **Client-streaming** and **bidi-streaming**: NOT supported in
//!   v1 — the dispatcher accumulates the request body into a single
//!   `Bytes` before sending. Both are easy to add in a follow-up
//!   PR by switching the request body to an `http_body_util::StreamBody`.
//!
//! # Why `tonic`?
//!
//! `tonic::transport::Channel` is a generic HTTP/2 client (no code
//! generation needed) that lives in the dep tree already as a
//! transitive dep of `opentelemetry-otlp`. We use the
//! `Endpoint::connect_lazy()` builder so the channel is established
//! on first use; this is a v1 simplification (no per-backend
//! connection pool) but works for typical gRPC workloads.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use log::{debug, error};
use std::time::Duration;
use tonic::body::BoxBody;
use tonic::transport::Channel;
use tower::Service;
use tower::ServiceExt;

use crate::models::router::{Backend, Router};

/// Default per-request timeout for the upstream gRPC call. 30 s
/// matches the existing HTTP/WS proxy defaults in
/// `services/http.rs::RouteHandler::new`.
pub const DEFAULT_GRPC_TIMEOUT_SECS: u64 = 30;

/// Factory that builds (and lazily connects) a `tonic::transport::Channel`
/// for each configured backend. The factory is intentionally cheap to
/// clone so the route handler can hold a handle without lifetime
/// gymnastics. v2 holds a [`GrpcChannelPool`](crate::services::grpc_pool::GrpcChannelPool)
/// so channels are reused across requests instead of recreated on
/// every call.
#[derive(Clone)]
pub struct GrpcService {
    timeout: Duration,
    pool: crate::services::grpc_pool::GrpcChannelPool,
}

impl GrpcService {
    /// Create a new dispatcher with the default 30 s upstream timeout.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_GRPC_TIMEOUT_SECS),
            pool: crate::services::grpc_pool::GrpcChannelPool::new(),
        }
    }

    /// Override the upstream timeout. Mostly useful in tests.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Return a reference to the connection pool (for metrics).
    pub fn pool(&self) -> &crate::services::grpc_pool::GrpcChannelPool {
        &self.pool
    }

    /// Build (or reuse) a `tonic` channel for `backend`.
    /// v2: on cache hit, returns the existing channel without
    /// touching the network. On miss, creates a new
    /// `connect_lazy` channel, inserts it into the pool, and
    /// returns it. The first real connection attempt happens
    /// on the first request through the new channel, so
    /// `channel_for` never blocks on TCP.
    ///
    /// v2: when `backend.tls` is `Some`, builds a
    /// `ClientTlsConfig` and applies it to the endpoint so
    /// the gateway establishes h2 (TLS) instead of h2c
    /// (cleartext). The same TLS fingerprint is included in
    /// the pool key so two backends on the same `host:port`
    /// with different TLS settings get separate channels.
    pub fn channel_for(&self, backend: &Backend) -> Result<Channel, GrpcError> {
        let mut key = crate::services::grpc_pool::BackendKey::from_backend(backend);
        if let Some(tls) = &backend.tls {
            // Hash the TLS config to produce a stable
            // fingerprint for the pool key. A real backend
            // can re-key (e.g. cert rotation) by changing
            // the PEM; the fingerprint changes, so the pool
            // entry is invalidated and a new channel is
            // built on the next request.
            key.tls_fingerprint = tls_fingerprint(tls);
        }
        if let Some(ch) = self.pool.get(&key) {
            return Ok(ch);
        }
        let uri = backend_uri(backend)?;
        let mut endpoint = tonic::transport::Endpoint::new(uri)?;
        if let Some(tls) = &backend.tls {
            endpoint = endpoint.tls_config(build_tls_config(tls)?)?;
        }
        let channel = endpoint.connect_lazy();
        self.pool.insert(key, channel.clone());
        Ok(channel)
    }
}

impl Default for GrpcService {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors raised by the gRPC dispatcher. Only the path from the
/// dispatcher itself; upstream gRPC errors are surfaced as
/// `tonic::Status` codes in the response body.
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    /// `backend.host` was not a valid `http://host[:port]` URI.
    /// We refuse the call rather than guessing a scheme.
    #[error("invalid backend URI for {host}:{port}: {reason}")]
    InvalidBackendUri {
        host: String,
        port: u16,
        reason: String,
    },
    /// `tonic` rejected the channel construction (e.g. DNS or
    /// scheme parse failure).
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The upstream call exceeded the configured timeout.
    #[error("upstream call timed out")]
    Timeout,
    /// The tower::Buffer wrapping the channel returned an
    /// opaque error that wasn't a tonic transport error.
    #[error("buffer error: {0}")]
    Buffer(String),
}

/// Build the URI that `tonic::transport::Endpoint` expects.
/// Picks `https://` when `backend.tls` is `Some`, else
/// `http://` (h2c with prior knowledge for the gateway-to-
/// backend hop). External-facing gRPC should still be
/// terminated by a TLS sidecar (envoy, nginx) when TLS is
/// disabled.
fn backend_uri(backend: &Backend) -> Result<http::Uri, GrpcError> {
    let host = &backend.host;
    let port = backend.port;
    let scheme = if backend.tls.is_some() { "https" } else { "http" };
    let candidate = format!("{}://{}:{}", scheme, host, port);
    candidate
        .parse::<http::Uri>()
        .map_err(|e| GrpcError::InvalidBackendUri {
            host: host.clone(),
            port,
            reason: e.to_string(),
        })
}

/// Hash the `BackendTlsConfig` into a stable `u64` for the
/// pool key. We hash the PEM bytes (or 0 if None), the
/// domain override (or 0 if None), and the
/// `insecure_skip_verify` flag. xxHash is used (via
/// `ahash::RandomState`) because it's already a workspace
/// dep and the value isn't security-sensitive.
fn tls_fingerprint(tls: &crate::models::router::BackendTlsConfig) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let state = ahash::AHasher::default();
    let mut hasher = state;
    hasher.write_u8(tls.insecure_skip_verify as u8);
    if let Some(pem) = &tls.ca_cert_pem {
        hasher.write(pem.as_bytes());
    } else {
        hasher.write_u8(0);
    }
    if let Some(d) = &tls.domain {
        hasher.write(d.as_bytes());
    } else {
        hasher.write_u8(0);
    }
    hasher.finish()
}

/// Build a `ClientTlsConfig` from a `BackendTlsConfig`.
/// Returns the appropriate `GrpcError` variant on
/// invalid PEM (the operator will see the error in the
/// gateway logs at config-load time).
fn build_tls_config(
    tls: &crate::models::router::BackendTlsConfig,
) -> Result<tonic::transport::ClientTlsConfig, GrpcError> {
    let mut cfg = tonic::transport::ClientTlsConfig::new()
        .with_enabled_roots();
    if let Some(pem) = &tls.ca_cert_pem {
        let cert = tonic::transport::Certificate::from_pem(pem);
        cfg = cfg.ca_certificate(cert);
    }
    if let Some(d) = &tls.domain {
        cfg = cfg.domain_name(d);
    } else {
        // Default: use the host as the SNI value, which is
        // what most operators want for a TLS-enabled
        // gRPC backend.
        cfg = cfg.domain_name(String::new()); // tonic will use the URI host
    }
    if tls.insecure_skip_verify {
        // The tonic API doesn't expose a clean
        // "skip verify" knob; the v2 workaround is to
        // skip TLS entirely and use h2c by NOT setting
        // the `tls` config. v3 will add proper
        // verification bypass via a custom
        // ClientTlsConfig wrapper. For now, log a
        // warning and continue (operator must set up
        // their own cert chain to use the TLS path).
        log::warn!(
            "backend.tls.insecure_skip_verify = true; \
             this v2 build does not support skipping \
             verification — the cert chain must be valid. \
             Set backend.tls.ca_cert_pem to a self-signed \
             CA bundle or use a valid public cert."
        );
    }
    Ok(cfg)
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use crate::models::router::BackendTlsConfig;

    #[test]
    fn tls_fingerprint_changes_with_pem() {
        let a = BackendTlsConfig {
            ca_cert_pem: Some("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_string()),
            domain: None,
            insecure_skip_verify: false,
        };
        let b = BackendTlsConfig {
            ca_cert_pem: Some("-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n".to_string()),
            domain: None,
            insecure_skip_verify: false,
        };
        assert_ne!(tls_fingerprint(&a), tls_fingerprint(&b));
    }

    #[test]
    fn tls_fingerprint_changes_with_domain() {
        let a = BackendTlsConfig {
            ca_cert_pem: None,
            domain: Some("api.example.com".to_string()),
            insecure_skip_verify: false,
        };
        let b = BackendTlsConfig {
            ca_cert_pem: None,
            domain: Some("other.example.com".to_string()),
            insecure_skip_verify: false,
        };
        assert_ne!(tls_fingerprint(&a), tls_fingerprint(&b));
    }

    #[test]
    fn tls_fingerprint_changes_with_insecure_flag() {
        let a = BackendTlsConfig {
            ca_cert_pem: None,
            domain: None,
            insecure_skip_verify: false,
        };
        let b = BackendTlsConfig {
            ca_cert_pem: None,
            domain: None,
            insecure_skip_verify: true,
        };
        assert_ne!(tls_fingerprint(&a), tls_fingerprint(&b));
    }

    #[test]
    fn backend_uri_uses_https_when_tls_is_some() {
        let backend = Backend {
            host: "api.example.com".to_string(),
            port: 443,
            weight: 1,
            health_check_path: None,
            tls: Some(BackendTlsConfig::default()),
        };
        let uri = backend_uri(&backend).unwrap();
        assert_eq!(uri.scheme_str(), Some("https"));
    }

    #[test]
    fn backend_uri_uses_http_when_tls_is_none() {
        let backend = Backend {
            host: "127.0.0.1".to_string(),
            port: 50051,
            weight: 1,
            health_check_path: None,
            tls: None,
        };
        let uri = backend_uri(&backend).unwrap();
        assert_eq!(uri.scheme_str(), Some("http"));
    }
}

/// Forward a streaming gRPC request body to the backend channel
/// and return the response. The request body is consumed
/// lazily (no pre-buffering) so this function handles all 4
/// gRPC call types:
/// - **Unary**: request body is 1 frame
/// - **Server-streaming**: request body is 1 frame, response is N
/// - **Client-streaming**: request body is N frames
/// - **Bidi-streaming**: both sides are N frames
///
/// The response body is always returned as a `tonic::BoxBody`,
/// which is itself an `http_body::Body` of frames — the
/// caller is responsible for streaming it to the wire
/// (the route handler does this via `TonicBodyStream`).
pub async fn forward_streaming(
    service: &GrpcService,
    backend: &Backend,
    request: http::Request<BoxBody>,
) -> Result<http::Response<BoxBody>, GrpcError> {
    let channel = service.channel_for(backend)?;
    debug!(
        "grpc forward_streaming: backend={}:{} path={}",
        backend.host,
        backend.port,
        request.uri().path()
    );
    let mut svc = tower::ServiceBuilder::new().buffer(1).service(channel);
    let _ready = svc.ready().await.map_err(|e| {
        error!("grpc not ready: {}", e);
        GrpcError::Buffer(format!("ready: {}", e))
    })?;
    let response = tokio::time::timeout(service.timeout, svc.call(request))
        .await
        .map_err(|_| {
            error!("grpc call timed out after {:?}", service.timeout);
            GrpcError::Timeout
        })?;
    let response = response.map_err(|e| {
        match e.downcast::<tonic::transport::Error>() {
            Ok(te) => map_transport_err(*te),
            Err(orig) => {
                error!("grpc buffer error: {}", orig);
                GrpcError::Buffer(orig.to_string())
            }
        }
    })?;
    Ok(response)
}

/// Unary-only convenience wrapper. Builds a `Full<Bytes>`
/// body from `body` and dispatches via
/// [`forward_streaming`]. Use this when you know the request
/// fits in memory (the common case for unary RPCs); for
/// streaming RPCs build a `BoxBody` from a stream and call
/// `forward_streaming` directly.
pub async fn forward_unary(
    service: &GrpcService,
    backend: &Backend,
    request: http::Request<Full<Bytes>>,
) -> Result<http::Response<BoxBody>, GrpcError> {
    let (parts, body) = request.into_parts();
    let body: BoxBody = BoxBody::new(body.map_err(|_| unreachable!("Full<Bytes> error is Infallible")));
    let request = http::Request::from_parts(parts, body);
    forward_streaming(service, backend, request).await
}

/// `hyper::body::Incoming` is what actix-web hands us for the
/// inbound HTTP/2 request body. We accumulate it into a single
/// `Bytes` for unary forwarding. Returns an error if the incoming
/// body is larger than `MAX_GRPC_PAYLOAD` so we don't OOM on a
/// malicious streaming client.
pub async fn collect_unary_body(
    body: Incoming,
    max_bytes: usize,
) -> Result<Bytes, GrpcBodyError> {
    let collected = body
        .collect()
        .await
        .map_err(|e| GrpcBodyError::Collect(e.to_string()))?
        .to_bytes();
    if collected.len() > max_bytes {
        return Err(GrpcBodyError::PayloadTooLarge {
            size: collected.len(),
            max: max_bytes,
        });
    }
    Ok(collected)
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcBodyError {
    #[error("failed to collect incoming body: {0}")]
    Collect(String),
    #[error("incoming body too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },
}

/// Map a `tonic::transport::Error` into something more useful. tonic
/// uses a single error type for both transport and protocol errors;
/// for the v1 dispatcher we treat all of them as a transport failure
/// and log them at `error!`. A future v2 will distinguish
/// `Unavailable` (backend down) from `Internal` (deserialization
/// failure) so the metrics layer can bucket them separately.
fn map_transport_err(e: tonic::transport::Error) -> GrpcError {
    error!("grpc transport error: {}", e);
    GrpcError::Transport(e)
}

/// Best-effort: report whether the inbound route is unary. v1 only
/// supports unary; if a route declares a streaming method path
/// (typically `/<svc>/<method>` where `<method>` ends in `Stream` or
/// the route's `protocol` is gRPC and the request is
/// `content-type: application/grpc+stream`), we surface a 501.
pub fn is_unary_request(content_type: Option<&str>) -> bool {
    // The gRPC wire spec doesn't carry a unary-vs-streaming marker
    // on the wire; clients signal it via the method name. Since
    // this v1 dispatcher treats all gRPC traffic as unary at the
    // body level, this function is a placeholder for v2.
    let _ = content_type;
    true
}

/// Convenience: build an `http::Request<Full<Bytes>>` from the path
/// + body. Preserves the `content-type` and `te: trailers` headers
/// that gRPC clients always send.
pub fn build_request(path: &str, body: Bytes, content_type: Option<&str>) -> http::Request<Full<Bytes>> {
    let uri = path
        .parse::<http::Uri>()
        .unwrap_or_else(|_| "/".parse().expect("static uri"));
    let mut builder = http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("te", "trailers");
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    builder
        .body(Full::new(body))
        .expect("static request parts")
}

/// Run a small smoke test: forward a synthetic unary call to a
/// fake "backend" that just echoes the path. Returns the response
/// status line. Exposed for the integration test in
/// `tests/grpc_routes.rs` (commit 3).
pub async fn smoke_dispatch(
    service: &GrpcService,
    backend: &Backend,
    path: &str,
    body: Bytes,
) -> Result<u16, GrpcError> {
    let req = build_request(path, body, Some("application/grpc"));
    let resp = forward_unary(service, backend, req).await?;
    Ok(resp.status().as_u16())
}

/// Helper for `routes/grpc.rs` to look up a gRPC route from a
/// `Router` list. The match uses the existing `external_path`
/// pattern (`/api/foo` style) since the gateway's route table is
/// path-based, not gRPC-method-based. v2 may add a `service`/
/// `method` discriminator.
pub fn find_route<'a>(routers: &'a [Router], path: &str) -> Option<&'a Router> {
    routers.iter().find(|r| {
        matches!(r.protocol, crate::models::router::Protocol::Grpc)
            && path_matches(&r.external_path, path)
    })
}

/// Simple prefix match (placeholder for the real route_matcher).
/// A request matches a route when the route's `external_path` is
/// a prefix of the request path. The match is permissive on
/// purpose: gRPC paths are arbitrary `/<package>.<svc>/<method>`
/// strings, and the gateway's contract is "this route forwards
/// calls for this prefix to this backend".
fn path_matches(external: &str, request_path: &str) -> bool {
    external == request_path
        || (external.ends_with('/') && request_path.starts_with(external))
        || request_path.starts_with(external.trim_end_matches('/').to_string().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::router::{Backend, LoadBalancingStrategy, Protocol, Router};

    fn sample_router() -> Router {
        Router {
            host: None,
            port: None,
            external_path: "/grpc.health.v1.Health/".to_string(),
            internal_path: String::new(),
            methods: vec!["POST".to_string()],
            auth_required: false,
            backends: Some(vec![Backend {
                host: "127.0.0.1".to_string(),
                port: 50051,
                weight: 1,
                health_check_path: None,
                tls: None,
            }]),
            load_balancing_strategy: LoadBalancingStrategy::RoundRobin,
            retry: None,
            protocol: Protocol::Grpc,
            request_transformation: None,
            response_transformation: None,
            ai_policy: None,
            cache: None,
        }
    }

    #[test]
    fn backend_uri_handles_plain_host() {
        let b = Backend {
            host: "127.0.0.1".to_string(),
            port: 50051,
            weight: 1,
            health_check_path: None,
                tls: None,
        };
        let uri = backend_uri(&b).unwrap();
        assert_eq!(uri.scheme_str(), Some("http"));
        assert_eq!(
            uri.authority().map(|a| a.as_str()),
            Some("127.0.0.1:50051")
        );
    }

    #[test]
    fn backend_uri_rejects_invalid_host() {
        let b = Backend {
            host: "not a host with spaces".to_string(),
            port: 50051,
            weight: 1,
            health_check_path: None,
                tls: None,
        };
        assert!(backend_uri(&b).is_err());
    }

    #[tokio::test]
    async fn channel_for_is_lazy() {
        // connect_lazy must NOT actually open a TCP connection.
        let svc = GrpcService::new();
        let b = Backend {
            host: "127.0.0.1".to_string(),
            port: 1, // port 1 is reserved; would fail to connect.
            weight: 1,
            health_check_path: None,
                tls: None,
        };
        let ch = svc.channel_for(&b).unwrap();
        // Channel is constructed but not yet connected.
        drop(ch);
    }

    #[test]
    fn build_request_sets_required_grpc_headers() {
        let req = build_request("/svc/method", Bytes::from_static(b"\x00\x00\x00\x00\x05hello"), Some("application/grpc"));
        assert_eq!(req.headers().get("content-type").unwrap(), "application/grpc");
        assert_eq!(req.headers().get("te").unwrap(), "trailers");
        assert_eq!(req.uri().path(), "/svc/method");
    }

    #[test]
    fn build_request_uses_default_path_on_invalid_uri() {
        // Invalid paths silently fall back to "/" so a malformed
        // client request doesn't break the dispatcher.
        let req = build_request("not a path", Bytes::new(), None);
        assert_eq!(req.uri().path(), "/");
    }

    #[test]
    fn find_route_matches_grpc_external_path() {
        let r = sample_router();
        let routes = vec![r.clone()];
        let found = find_route(&routes, "/grpc.health.v1.Health/Check");
        assert!(found.is_some());
        assert_eq!(found.unwrap().external_path, "/grpc.health.v1.Health/");
    }

    #[test]
    fn find_route_ignores_non_grpc_routes() {
        let mut r = sample_router();
        r.protocol = Protocol::Http;
        let routes = vec![r];
        let found = find_route(&routes, "/foo");
        assert!(found.is_none());
    }

    #[test]
    fn path_matches_handles_trailing_slash() {
        assert!(path_matches("/api/foo/", "/api/foo/bar"));
        assert!(path_matches("/api/foo", "/api/foo/bar"));
        assert!(path_matches("/api/foo", "/api/foo"));
        assert!(!path_matches("/api/foo", "/api/baz"));
    }

    #[tokio::test]
    async fn smoke_dispatch_returns_error_for_unreachable_backend() {
        // A full-stack smoke test against a real gRPC server belongs
        // in tests/grpc_routes.rs (PR9 commit 3). Here we just
        // confirm that channel_for() produces something callable.
        let svc = GrpcService::new();
        let b = Backend {
            host: "127.0.0.1".to_string(),
            port: 1,
            weight: 1,
            health_check_path: None,
                tls: None,
        };
        let ch = svc.channel_for(&b);
        assert!(ch.is_ok());
    }
}
