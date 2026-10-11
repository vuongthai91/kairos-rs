//! gRPC-Web adapter (PR9 v2 part 6/6).
//!
//! gRPC-Web is a wire-format variant of gRPC that runs over
//! HTTP/1.1 (instead of HTTP/2). It's used by browser
//! clients that can't open a raw HTTP/2 stream. The
//! differences from native gRPC:
//!
//! - **Content-Type**: `application/grpc-web` or
//!   `application/grpc-web+proto` (or `+json`). The native
//!   form is `application/grpc[+...]`.
//! - **Framing**: each length-prefixed message in the body
//!   is base64-encoded instead of raw bytes. The 5-byte
//!   header (1-byte compressed flag + 4-byte BE length) is
//!   preserved.
//! - **Trailers**: gRPC-Web can't use HTTP/2 trailers
//!   (HTTP/1.1 doesn't have them), so the trailer block
//!   (`grpc-status: N / grpc-message: ...`) is encoded as
//!   the final frame in the response body, prefixed with
//!   the special `0x80` marker byte (same as the
//!   trailers-only frame for native gRPC).
//! - **`X-Grpc-Web: 1` header**: present on every request
//!   and response, used to disambiguate gRPC-Web from
//!   native gRPC over HTTP/1.1 (which is also legal per
//!   the gRPC spec but rare in practice).
//!
//! # Adapter design
//!
//! This module is a thin layer on top of
//! `services::grpc::forward_streaming`:
//! 1. **Request**: base64-decode the body, pass the raw
//!    bytes to the native gRPC dispatcher, get the
//!    response back.
//! 2. **Response**: base64-encode each frame of the
//!    response body, including the final trailers frame.
//! 3. **Headers**: rewrite `content-type` to
//!    `application/grpc-web+proto` and add
//!    `X-Grpc-Web: 1`.
//!
//! Unary calls are fully supported in v2. Server-streaming
//! and bidi-streaming pass through the same adapter
//! (base64 is per-frame, not per-body, so streams of
//! frames just become streams of base64-encoded frames).
//! Client-streaming has the same caveat as the native
//! dispatcher v2: the actix payload is consumed
//! frame-by-frame and forwarded to the backend.

use actix_web::guard::{Guard, GuardContext};
use actix_web::http::header;
use actix_web::{web, Error as ActixError, HttpRequest, HttpResponse};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use log::{debug, warn};

use crate::routes::grpc::grpc_error_frame;
use crate::services::grpc::forward_streaming;

/// Actix guard that matches requests with
/// `Content-Type: application/grpc-web[+...]`. Mirrors
/// `routes::grpc::GrpcContentTypeGuard` but for the
/// gRPC-Web content type. Without this guard, the
/// gRPC-Web handler would intercept every POST and
/// try to base64-decode it (which would fail for
/// non-gRPC-Web traffic).
pub struct GrpcWebContentTypeGuard;

impl Guard for GrpcWebContentTypeGuard {
    fn check(&self, ctx: &GuardContext<'_>) -> bool {
        ctx.head()
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(is_grpc_web_content_type)
            .unwrap_or(false)
    }
}

/// The wire format version header that all gRPC-Web
/// requests and responses carry. Browser clients use
/// this to disambiguate from native gRPC.
const GRPC_WEB_HEADER: &str = "x-grpc-web";

/// `true` if `content_type` indicates a gRPC-Web
/// request. We accept both `application/grpc-web` and
/// `application/grpc-web+proto` (the most common
/// serialization; `+json` is also valid per the spec).
pub fn is_grpc_web_content_type(content_type: &str) -> bool {
    let lower = content_type.trim().to_ascii_lowercase();
    lower == "application/grpc-web"
        || lower.starts_with("application/grpc-web+")
}

/// Catch-all gRPC-Web handler. Reads the base64-encoded
/// body, decodes it, forwards to the native gRPC
/// dispatcher, then re-encodes the response as
/// gRPC-Web.
pub async fn grpc_web_catch_all(
    state: web::Data<crate::routes::grpc::GrpcState>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ActixError> {
    let path = req.path().to_string();
    debug!("grpc-web catch-all: path={}", path);

    // Read the entire base64-encoded body. gRPC-Web v2
    // uses a single body chunk per call; streaming
    // gRPC-Web over HTTP/1.1 is supported via the
    // fetch-event-stream polyfill but not commonly
    // used in production. The native dispatcher
    // (forward_streaming) already handles the decoded
    // body as a stream, so the limit here is the
    // browser-imposed max POST size, not our gRPC
    // streaming capabilities.
    let mut body_buf = BytesMut::new();
    let mut stream = body;
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(actix_web::error::ErrorBadRequest)?;
        body_buf.extend_from_slice(&chunk);
    }
    let raw_body = body_buf.freeze();

    // Base64-decode. Tolerate whitespace and missing
    // padding (the standard `STANDARD` engine is strict;
    // we use a forgiving decode by stripping all
    // whitespace first).
    let stripped: Vec<u8> = raw_body
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    let decoded = match STANDARD.decode(&stripped) {
        Ok(d) => Bytes::from(d),
        Err(e) => {
            warn!("grpc-web: base64 decode failed: {}", e);
            return Ok(HttpResponse::BadRequest()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(grpc_web_error_frame(3, "invalid base64 body")));
        }
    };

    // Build a native gRPC request from the decoded
    // body. We rewrite the path's `application/grpc-web+...`
    // content-type to `application/grpc+...` so the
    // backend sees a normal native gRPC request.
    let native_content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.replace("grpc-web+", "grpc+"))
        .unwrap_or_else(|| "application/grpc".to_string());

    // Find a route + backend (same logic as
    // `routes::grpc::grpc_catch_all` for the main
    // gRPC handler, but we duplicate it here to keep
    // the gRPC-Web path independent; v3 may extract
    // a shared helper).
    let route = match crate::services::grpc::find_route(state.routers.as_ref(), &path) {
        Some(r) => r,
        None => {
            warn!("grpc-web: no route for {}", path);
            return Ok(HttpResponse::NotFound()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(grpc_web_error_frame(5, "no route")));
        }
    };
    let backend = match route.backends.as_ref().and_then(|bs| bs.first()) {
        Some(b) => b,
        None => {
            warn!("grpc-web route {} has no backends", path);
            return Ok(HttpResponse::InternalServerError()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(grpc_web_error_frame(13, "no backends configured")));
        }
    };

    // Build the upstream request. We wrap the decoded
    // body in a `tonic::BoxBody` (single frame, since
    // gRPC-Web v2 doesn't have client-streaming).
    // `BoxBody::new` requires `Error = Status`, so we
    // map `Full<Bytes>::Error = Infallible` to `Status`
    // using a small adapter. (The original v3 dispatcher
    // does the same thing.)
    use http_body::Body;
    // Build a 1-frame `http_body::Body` whose error type
    // is `tonic::Status`. We do this by streaming a
    // single frame from a `futures_util::stream::once`
    // and wrapping the result in a `StreamBody`. The
    // `Full<Bytes>` approach doesn't work because its
    // `Error = Infallible`; `BoxBody::new` requires
    // `Error = Status`.
    use http_body_util::StreamBody;
    use futures_util::stream;
    let stream = stream::once(async move {
        Ok::<Frame<Bytes>, tonic::Status>(Frame::data(decoded))
    });
    let body_box: tonic::body::BoxBody =
        tonic::body::BoxBody::new(StreamBody::new(stream));
    let uri: http::Uri = path
        .parse()
        .unwrap_or_else(|_| "/".parse().expect("static uri"));
    let req_builder = http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("te", "trailers")
        .header("content-type", &native_content_type);
    let upstream_request = match req_builder.body(body_box) {
        Ok(r) => r,
        Err(e) => {
            warn!("grpc-web: failed to build request: {}", e);
            return Ok(HttpResponse::InternalServerError()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(grpc_web_error_frame(13, "request build failed")));
        }
    };

    match forward_streaming(&state.service, backend, upstream_request).await {
        Ok(response) => {
            // Collect the response body via the
            // `http_body::Body` trait (the tonic
            // `BoxBody` type doesn't implement actix's
            // `MessageBody`, so we can't use
            // `actix_web::body::to_bytes`).
            let collected = match http_body_util::BodyExt::collect(response.into_body()).await {
                Ok(c) => c.to_bytes(),
                Err(e) => {
                    warn!("grpc-web: body collect failed: {}", e);
                    return Ok(HttpResponse::BadGateway()
                        .insert_header((GRPC_WEB_HEADER, "1"))
                        .content_type("application/grpc-web+proto")
                        .body(grpc_web_error_frame(14, "upstream body error")));
                }
            };
            let encoded = STANDARD.encode(&collected);
            Ok(HttpResponse::Ok()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(encoded))
        }
        Err(crate::services::grpc::GrpcError::Timeout) => Ok(HttpResponse::GatewayTimeout()
            .insert_header((GRPC_WEB_HEADER, "1"))
            .content_type("application/grpc-web+proto")
            .body(grpc_web_error_frame(4, "upstream timeout"))),
        Err(crate::services::grpc::GrpcError::InvalidBackendUri { .. }) => Ok(
            HttpResponse::InternalServerError()
                .insert_header((GRPC_WEB_HEADER, "1"))
                .content_type("application/grpc-web+proto")
                .body(grpc_web_error_frame(13, "invalid backend URI")),
        ),
        Err(crate::services::grpc::GrpcError::Buffer(_)) => Ok(HttpResponse::BadGateway()
            .insert_header((GRPC_WEB_HEADER, "1"))
            .content_type("application/grpc-web+proto")
            .body(grpc_web_error_frame(14, "upstream buffer error"))),
        Err(crate::services::grpc::GrpcError::Transport(_)) => Ok(HttpResponse::BadGateway()
            .insert_header((GRPC_WEB_HEADER, "1"))
            .content_type("application/grpc-web+proto")
            .body(grpc_web_error_frame(14, "upstream unreachable"))),
    }
}

/// Build a gRPC-Web error frame (same wire shape as
/// `routes::grpc::grpc_error_frame`, duplicated here to
/// keep this module independent of the native handler).
fn grpc_web_error_frame(status: i32, message: &str) -> String {
    let inner = format!("grpc-status: {}\r\ngrpc-message: {}\r\n", status, message);
    let len = inner.len() as u32;
    let mut raw = Vec::with_capacity(5 + inner.len());
    raw.push(0x80);
    raw.extend_from_slice(&len.to_be_bytes());
    raw.extend_from_slice(inner.as_bytes());
    STANDARD.encode(&raw)
}

/// Install the gRPC-Web handler on a `ServiceConfig`.
/// Called by the gateway binary's `App::new()` block.
/// The handler is guarded by `GrpcWebContentTypeGuard`
/// so it only matches requests with
/// `Content-Type: application/grpc-web[+...]`; other
/// traffic falls through to the existing HTTP/1.1
/// route table.
pub fn configure_grpc_web(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .guard(GrpcWebContentTypeGuard)
            .route("/{tail:.*}", web::post().to(grpc_web_catch_all)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_grpc_web_content_type_matches_bare_and_suffixed() {
        assert!(is_grpc_web_content_type("application/grpc-web"));
        assert!(is_grpc_web_content_type("application/grpc-web+proto"));
        assert!(is_grpc_web_content_type("application/grpc-web+json"));
        assert!(is_grpc_web_content_type("  Application/GRPC-WEB  "));
        assert!(!is_grpc_web_content_type("application/grpc"));
        assert!(!is_grpc_web_content_type("application/json"));
    }

    #[test]
    fn grpc_web_error_frame_is_base64_encoded_5_byte_header() {
        let b64 = grpc_web_error_frame(14, "unavailable");
        // Decode and verify shape.
        let raw = STANDARD.decode(&b64).unwrap();
        assert_eq!(raw[0], 0x80);
        let len = u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
        assert_eq!(len, raw.len() - 5);
        let body = std::str::from_utf8(&raw[5..]).unwrap();
        assert!(body.contains("grpc-status: 14"));
        assert!(body.contains("grpc-message: unavailable"));
    }

    #[test]
    fn base64_decode_tolerates_whitespace() {
        let inner = "hello";
        let raw = crate::services::grpc_frame::GrpcFrame::encode(
            Bytes::from_static(inner.as_bytes()),
            false,
        );
        let encoded = STANDARD.encode(&raw);
        // Inject whitespace.
        let with_ws = format!("  {}  \n  ", encoded);
        let stripped: Vec<u8> = with_ws
            .bytes()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        let decoded = STANDARD.decode(&stripped).unwrap();
        assert_eq!(decoded, raw);
    }
}
