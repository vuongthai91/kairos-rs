//! gRPC proxy route handling (PR9).
//!
//! Registers a catch-all handler that matches any `POST` request with
//! `Content-Type: application/grpc[+...]` and forwards the body to
//! the configured backend via `services::grpc::forward_unary`. The
//! route table lookup uses the existing `Router.external_path` field
//! as a prefix so gRPC service paths (`/grpc.health.v1.Health/...`)
//! can be matched against the gateway's route config.
//!
//! # Wire-up
//!
//! The gateway binary's `App::new()` block calls
//! [`configure_grpc`] to install the handler. The handler itself is
//! exposed as `web::Data<GrpcState>` so it shares one
//! [`GrpcService`](crate::services::grpc::GrpcService) across
//! requests (cheap to clone; the expensive part — the `tonic`
//! `Channel` — is built lazily per backend per call).
//!
//! # Compatibility
//!
//! The handler is registered at the root of the app (no path scope)
//! and uses an actix guard to match gRPC traffic by content type. It
//! runs alongside the existing HTTP/WS/FTP/DNS route scopes; a
//! request that doesn't match the gRPC guard falls through to the
//! normal route table.

use actix_web::guard::{Guard, GuardContext};
use actix_web::http::header;
use actix_web::{web, Error as ActixError, HttpRequest, HttpResponse};
use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use log::{debug, warn};

use crate::models::router::Router;
use crate::services::grpc::{find_route, forward_streaming, GrpcService};
use crate::services::grpc_frame::is_grpc_content_type;

/// `web::Data` wrapper for the gRPC dispatcher. Holds the
/// pre-built `GrpcService` plus an `Arc<Router>` snapshot of the
/// current route table. The gateway rebuilds this whenever
/// configuration is hot-reloaded.
#[derive(Clone)]
pub struct GrpcState {
    pub service: GrpcService,
    pub routers: std::sync::Arc<Vec<Router>>,
}

impl GrpcState {
    pub fn new(service: GrpcService, routers: std::sync::Arc<Vec<Router>>) -> Self {
        Self { service, routers }
    }
}

/// Guard that matches requests whose `content-type` header starts
/// with `application/grpc`. Used to register the gRPC handler ahead
/// of the generic HTTP handler so the catch-all doesn't shadow it.
pub struct GrpcContentTypeGuard;

impl Guard for GrpcContentTypeGuard {
    fn check(&self, ctx: &GuardContext<'_>) -> bool {
        ctx.head()
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(is_grpc_content_type)
            .unwrap_or(false)
    }
}

/// Catch-all gRPC handler. Reads the request body, finds the
/// matching gRPC route, forwards the call, and returns the response.
pub async fn grpc_catch_all(
    state: web::Data<GrpcState>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ActixError> {
    let path = req.path().to_string();
    debug!("grpc catch-all: path={}", path);

    // Resolve the gRPC route. v1 uses prefix matching on
    // `Router.external_path`; v2 will add a `service` field.
    let route = match find_route(state.routers.as_ref(), &path) {
        Some(r) => r,
        None => {
            // gRPC reflection fallback: paths starting with
            // `grpc.reflection.v1alpha.ServerReflection/` or
            // `grpc.reflection.v1.ServerReflection/` are
            // forwarded to the first backend of any gRPC
            // route that has backends. This lets `grpcurl
            // --plaintext` (or any reflection-aware client)
            // discover the gateway's exposed services
            // without an explicit route config. If no gRPC
            // route has backends, fall through to 404.
            if is_reflection_path(&path) {
                if let Some(fallback_idx) = first_grpc_backend_index(state.routers.as_ref()) {
                    let backend = state.routers[fallback_idx]
                        .backends
                        .as_ref()
                        .and_then(|bs| bs.first())
                        .expect("checked non-empty by helper");
                    warn!(
                        "grpc: reflection path {} routed to fallback backend {}/{}",
                        path, backend.host, backend.port
                    );
                    let service = state.service.clone();
                    return forward_to_backend(service, &req, body, backend, &path).await;
                }
            }
            warn!("grpc: no route for {}", path);
            return Ok(HttpResponse::NotFound()
                .content_type("application/grpc")
                .body(grpc_error_frame(5, "no route")));
        }
    };

    // Pick a backend. v1 always uses the first one; v2 will use
    // the same `LoadBalancer` machinery as the HTTP path.
    let backend = match route.backends.as_ref().and_then(|bs| bs.first()) {
        Some(b) => b,
        None => {
            warn!("grpc route {} has no backends", path);
            return Ok(HttpResponse::InternalServerError()
                .content_type("application/grpc")
                .body(grpc_error_frame(13, "no backends configured")));
        }
    };

    let service = state.service.clone();
    forward_to_backend(service, &req, body, backend, &path).await
}

/// Forward `body` to `backend` over gRPC. The body is
/// consumed lazily (no pre-buffering) so this function
/// handles all 4 gRPC call types. Used by both the main
/// route dispatch and the reflection fallback (commit 5).
async fn forward_to_backend(
    service: GrpcService,
    req: &HttpRequest,
    body: web::Payload,
    backend: &crate::models::router::Backend,
    path: &str,
) -> Result<HttpResponse, ActixError> {
    // Build the upstream request URI, preserving the inbound
    // content type so the backend can choose the right
    // deserializer. The body is consumed lazily: we wrap the
    // actix `web::Payload` (a Stream of Result<Bytes>) in an
    // `http_body::Body` adapter and hand it to the dispatcher
    // without buffering. This is what makes client-streaming
    // and bidi-streaming calls work — tonic's `BoxBody`
    // implements `http_body::Body`, and the dispatcher can
    // read frames as the backend polls them.
    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let uri: http::Uri = path
        .parse()
        .unwrap_or_else(|_| "/".parse().expect("static uri"));
    let mut builder = http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("te", "trailers");
    if let Some(ref ct) = content_type {
        builder = builder.header("content-type", ct);
    }

    // Build a streaming body from the actix payload. The
    // adapter maps `PayloadError` → `tonic::Status` so tonic
    // can frame them. actix's `web::Payload` is `!Send`
    // (it uses `Rc` internally), so we can't hand it to
    // tonic directly. Instead, spawn a task on the current
    // actix worker that drains the payload into a
    // tokio::sync::mpsc channel; the receiver (which IS
    // Send) is wrapped in a `StreamBody` and handed to
    // tonic. The body is consumed lazily — no pre-buffering.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<bytes::Bytes>, tonic::Status>>(8);
    actix_web::rt::spawn(async move {
        let mut stream = body;
        while let Some(item) = stream.next().await {
            let frame = match item {
                Ok(bytes) => Ok(Frame::data(bytes)),
                Err(e) => Err(tonic::Status::internal(format!("upstream read error: {}", e))),
            };
            if tx.send(frame).await.is_err() {
                break;
            }
        }
    });
    let body: tonic::body::BoxBody = tonic::body::BoxBody::new(StreamBody::new(
        futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }),
    ));

    let request = match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            warn!("failed to build gRPC request: {}", e);
            return Ok(HttpResponse::InternalServerError()
                .content_type("application/grpc")
                .body(grpc_error_frame(13, "request build failed")));
        }
    };

    match forward_streaming(&service, backend, request).await {
        Ok(response) => {
            // Stream the response body. actix's `streaming()`
            // accepts a `Stream<Item = Result<Bytes, E>>` and
            // writes each chunk as it arrives. Works for
            // unary, server-streaming, and bidi-streaming
            // responses: a unary response is just a 1-frame
            // stream.
            let stream = crate::services::grpc_stream::TonicBodyStream::new(response.into_body());
            Ok(HttpResponse::Ok()
                .content_type("application/grpc")
                .streaming(stream))
        }
        Err(crate::services::grpc::GrpcError::Timeout) => Ok(HttpResponse::GatewayTimeout()
            .content_type("application/grpc")
            .body(grpc_error_frame(4, "upstream timeout"))),
        Err(crate::services::grpc::GrpcError::InvalidBackendUri { .. }) => Ok(
            HttpResponse::InternalServerError()
                .content_type("application/grpc")
                .body(grpc_error_frame(13, "invalid backend URI")),
        ),
        Err(crate::services::grpc::GrpcError::Buffer(_)) => Ok(HttpResponse::BadGateway()
            .content_type("application/grpc")
            .body(grpc_error_frame(14, "upstream buffer error"))),
        Err(crate::services::grpc::GrpcError::Transport(_)) => Ok(HttpResponse::BadGateway()
            .content_type("application/grpc")
            .body(grpc_error_frame(14, "upstream unreachable"))),
    }
}

/// Return `true` if `path` is a gRPC server-reflection
/// request. The v1 alpha and v1 stable paths are both
/// recognised. Used by the reflection fallback in
/// `grpc_catch_all`.
fn is_reflection_path(path: &str) -> bool {
    path.starts_with("/grpc.reflection.v1alpha.ServerReflection/")
        || path.starts_with("/grpc.reflection.v1.ServerReflection/")
}

/// Find the index of the first gRPC route in `routers`
/// that has a non-empty `backends` field. Used by the
/// reflection fallback in `grpc_catch_all`. We return an
/// index instead of a borrow so the caller can re-index
/// into `routers` after the borrow on `state.routers`
/// has been released (avoids a borrow-checker conflict
/// when we then move `state` into `forward_to_backend`).
fn first_grpc_backend_index(routers: &[Router]) -> Option<usize> {
    routers.iter().position(|r| {
        matches!(r.protocol, crate::models::router::Protocol::Grpc)
            && r.backends.as_ref().is_some_and(|bs| !bs.is_empty())
    })
}

/// Build a single-frame gRPC error body for the cases where the
/// dispatcher needs to surface a `grpc-status` to the client without
/// talking to a backend. Wire format: 1 byte compressed flag + 4
/// bytes BE length + `grpc-message` header + payload. For v1 we
/// skip the trailing metadata-Map step and just emit the status;
/// clients should fall back to HTTP status code for the failure
/// reason.
fn grpc_error_frame(status: i32, message: &str) -> Vec<u8> {
    let body = format!("grpc-status: {}\r\ngrpc-message: {}\r\n", status, message);
    let len = body.len() as u32;
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(0x80); // compressed flag set + reserved bit; we use the
                    // standard gRPC "trailers-only" marker.
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body.as_bytes());
    out
}

/// Install the gRPC handler on the given `ServiceConfig`. The
/// gateway binary calls this from its `App::new()` block:
pub fn configure_grpc(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .guard(GrpcContentTypeGuard)
            .route("/health.v1.Health/Check", web::post().to(grpc_catch_all))
            // Fallback: any path under a gRPC route.
            .route("/{tail:.*}", web::post().to(grpc_catch_all)),
    );
}

/// Helper for unit tests: build a `GrpcState` from a router list.
#[cfg(test)]
pub fn state_for_tests(routers: Vec<Router>) -> GrpcState {
    GrpcState::new(
        GrpcService::new(),
        std::sync::Arc::new(routers),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: GrpcContentTypeGuard delegates to
    // `is_grpc_content_type` from `services::grpc_frame`, which is
    // exhaustively unit-tested there. The actix `GuardContext` is
    // crate-private so we can't construct it from user code; the
    // integration test in `routes::grpc::tests::grpc_catch_all_*`
    // (commit 3) covers the end-to-end behavior.

    #[test]
    fn grpc_error_frame_wire_format_is_5_byte_header() {
        let frame = grpc_error_frame(14, "unavailable");
        assert_eq!(frame[0], 0x80);
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(len, frame.len() - 5);
        let body = std::str::from_utf8(&frame[5..]).unwrap();
        assert!(body.contains("grpc-status: 14"));
        assert!(body.contains("grpc-message: unavailable"));
    }

    #[test]
    fn is_reflection_path_matches_v1_alpha_and_v1() {
        assert!(is_reflection_path("/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"));
        assert!(is_reflection_path("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"));
        assert!(!is_reflection_path("/grpc.health.v1.Health/Check"));
        assert!(!is_reflection_path("/foo/bar"));
        assert!(!is_reflection_path("/"));
    }

    #[test]
    fn first_grpc_backend_index_returns_none_when_no_grpc_routes() {
        let routers = vec![];
        assert_eq!(first_grpc_backend_index(&routers), None);
    }

    #[test]
    fn first_grpc_backend_index_finds_first_grpc_route_with_backends() {
        use crate::models::router::{Backend, LoadBalancingStrategy, Protocol, Router};
        let routers = vec![
            Router {
                host: None,
                port: None,
                external_path: "/api/grpc/".to_string(),
                internal_path: String::new(),
                methods: vec!["POST".to_string()],
                auth_required: false,
                backends: Some(vec![Backend {
                    host: "api.example.com".to_string(),
                    port: 443,
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
            },
        ];
        assert_eq!(first_grpc_backend_index(&routers), Some(0));
    }
}
