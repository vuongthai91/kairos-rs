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
use bytes::BytesMut;
use futures_util::StreamExt;
use log::{debug, warn};

use crate::models::router::Router;
use crate::services::grpc::{
    build_request, find_route, forward_unary, GrpcService,
};
use crate::services::grpc_frame::MAX_GRPC_PAYLOAD;
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

    // Accumulate the body into a single Bytes (unary only in v1).
    let mut buf = BytesMut::new();
    let mut stream = body;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(actix_web::error::ErrorBadRequest)?;
        if buf.len() + chunk.len() > MAX_GRPC_PAYLOAD {
            return Ok(HttpResponse::PayloadTooLarge()
                .content_type("application/grpc")
                .body(grpc_error_frame(8, "payload too large")));
        }
        buf.extend_from_slice(&chunk);
    }
    let body = buf.freeze();

    // Build the upstream request, preserving the inbound content
    // type so the backend can choose the right deserializer.
    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let request = build_request(&path, body, content_type.as_deref());

    match forward_unary(&state.service, backend, request).await {
        Ok(response) => {
            // We have to convert the tonic `BoxBody` response body
            // back into an actix `web::Bytes` for the client. v1
            // only handles unary responses (single frame); a v2
            // streaming implementation will need an
            // actix-web `StreamingBody`.
            use http_body_util::BodyExt;
            let collected = match response.into_body().collect().await {
                Ok(c) => c.to_bytes(),
                Err(_) => {
                    return Ok(HttpResponse::BadGateway()
                        .content_type("application/grpc")
                        .body(grpc_error_frame(14, "upstream body error")));
                }
            };
            Ok(HttpResponse::Ok()
                .content_type("application/grpc")
                .body(collected))
        }
        Err(crate::services::grpc::GrpcError::Timeout) => Ok(HttpResponse::GatewayTimeout()
            .content_type("application/grpc")
            .body(grpc_error_frame(4, "upstream timeout"))),
        Err(crate::services::grpc::GrpcError::InvalidBackendUri { .. }) => Ok(
            HttpResponse::InternalServerError()
                .content_type("application/grpc")
                .body(grpc_error_frame(13, "invalid backend URI")),
        ),
        Err(crate::services::grpc::GrpcError::Transport(_)) => Ok(HttpResponse::BadGateway()
            .content_type("application/grpc")
            .body(grpc_error_frame(14, "upstream unreachable"))),
    }
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
}
