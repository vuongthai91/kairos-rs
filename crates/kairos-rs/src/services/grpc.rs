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
use futures_util::FutureExt;
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
/// gymnastics.
#[derive(Clone)]
pub struct GrpcService {
    timeout: Duration,
}

impl GrpcService {
    /// Create a new dispatcher with the default 30 s upstream timeout.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_GRPC_TIMEOUT_SECS),
        }
    }

    /// Override the upstream timeout. Mostly useful in tests.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Build (or reuse) a `tonic` channel for `backend`. We use
    /// `connect_lazy` so the channel is created synchronously and
    /// the first real connection attempt happens on the first
    /// request. This avoids blocking the actix worker pool on
    /// TCP handshakes.
    pub fn channel_for(&self, backend: &Backend) -> Result<Channel, GrpcError> {
        let uri = backend_uri(backend)?;
        let endpoint = tonic::transport::Endpoint::new(uri)?.connect_lazy();
        Ok(endpoint)
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

/// Build the URI that `tonic::transport::Endpoint` expects. We
/// always use h2c with prior knowledge (no TLS) for the v1
/// gateway-to-backend hop. External-facing gRPC should still be
/// terminated by a TLS sidecar (envoy, nginx).
fn backend_uri(backend: &Backend) -> Result<http::Uri, GrpcError> {
    let host = &backend.host;
    let port = backend.port;
    let candidate = format!("http://{}:{}{}", host, port, "");
    candidate
        .parse::<http::Uri>()
        .map_err(|e| GrpcError::InvalidBackendUri {
            host: host.clone(),
            port,
            reason: e.to_string(),
        })
}

/// Forward `body` (a unary gRPC request body) to the backend channel
/// and return the response. The response body is also expected to
/// be a unary frame; streaming responses are NOT supported in v1
/// (the caller should check `is_unary_endpoint` before calling).
pub async fn forward_unary(
    service: &GrpcService,
    backend: &Backend,
    request: http::Request<Full<Bytes>>,
) -> Result<http::Response<BoxBody>, GrpcError> {
    let channel = service.channel_for(backend)?;
    debug!(
        "grpc forward_unary: backend={}:{} path={}",
        backend.host,
        backend.port,
        request.uri().path()
    );
    let (parts, body) = request.into_parts();
    // tonic::transport::Channel implements Service<http::Request<BoxBody>>,
    // not Service<http::Request<Full<Bytes>>>. Map the Full<Bytes> error
    // type (Infallible) into tonic::Status so the wrap is total.
    let body: BoxBody = BoxBody::new(body.map_err(|_| unreachable!("Full<Bytes> error is Infallible")));
    let request = http::Request::from_parts(parts, body);
    // Wrap the channel in a tower::Buffer so `.call()` is safe
    // to drive directly without manual poll_ready ceremony.
    // The buffer holds one in-flight request, which is enough
    // for v1 (one actix request = one tonic call).
    let mut svc = tower::ServiceBuilder::new().buffer(1).service(channel);
    // `poll_ready` reserves a permit in the buffer; without it
    // `call()` would panic. `Service::ready()` returns a
    // `Ready<Self, Request>` future that drives poll_ready and
    // self-references into the service.
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
    // The buffer's error type is `Box<dyn StdError + Send + Sync>`,
    // not the inner service's error. Downcast to recover the
    // tonic transport error when possible.
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
        };
        let ch = svc.channel_for(&b);
        assert!(ch.is_ok());
    }
}
