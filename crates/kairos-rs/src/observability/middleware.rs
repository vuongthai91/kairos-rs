//! Actix-web middleware that creates a root tracing span for every
//! request, propagates W3C `traceparent` from incoming headers into
//! the span's parent context, and stamps `X-Trace-Id` / `X-Span-Id`
//! on the response so clients can correlate with backend logs.
//!
//! # When tracing is disabled
//!
//! When [`super::tracing::init_telemetry`] has not been called, all
//! the helpers in `tracing.rs` short-circuit to no-ops:
//! [`super::tracing::extract_context_from_request`] returns `None`
//! and [`super::tracing::stamp_response_headers`] writes nothing.
//! The middleware still creates a `tracing::Span` (so it doesn't
//! need a separate code path), but the bridge to OpenTelemetry is
//! absent and the span is dropped on scope exit without being
//! exported. The cost is one `tracing::info_span!` call per request.
//!
//! # Wire-up
//!
//! ```ignore
//! use kairos_rs::observability::middleware::TracingMiddleware;
//! App::new().wrap(TracingMiddleware)
//! ```
//!
//! Place it as the **outermost** `.wrap(...)` so the span covers
//! every other middleware (auth, rate-limit, compression, ...).
//! In `crates/kairos-gateway/src/main.rs` we register it before
//! `actix_web::middleware::Logger` for that reason.

use std::future::{ready, Future, Ready};
use std::pin::Pin;

use actix_web::body::MessageBody;
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::tracing::{extract_context_from_request, stamp_response_headers};

/// Middleware factory. Cheap to construct — the real work happens
/// when actix calls [`Transform::new_transform`] to wrap a service.
pub struct TracingMiddleware;

impl<S, B> Transform<S, ServiceRequest> for TracingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = TracingMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TracingMiddlewareService { service }))
    }
}

/// Per-service instance. Holds the inner service so we can delegate
/// `poll_ready` and call it once the span is set up.
pub struct TracingMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for TracingMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Snapshot request metadata up front — the values are needed
        // for both the span attributes (before the await) and the
        // response stamping (after the await).
        let method = req.method().to_string();
        let path = req.path().to_string();
        let scheme = req.connection_info().scheme().to_string();
        let host = req.connection_info().host().to_string();
        let user_agent = req
            .headers()
            .get(actix_web::http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let remote_addr = req
            .connection_info()
            .realip_remote_addr()
            .unwrap_or("")
            .to_string();

        // Pull the W3C Trace Context from the incoming request
        // headers. `None` when tracing is disabled OR the client
        // didn't send `traceparent`. In the latter case the new span
        // will start a fresh trace (subject to sampling).
        let parent_cx = extract_context_from_request(req.headers());

        // Build the root span for this request. `http.status_code`
        // is recorded after the response comes back — using
        // `tracing::field::Empty` reserves the slot in the OTel span.
        let span = tracing::info_span!(
            "http.request",
            http.method = %method,
            http.target = %path,
            http.scheme = %scheme,
            http.host = %host,
            http.user_agent = %user_agent,
            http.client_ip = %remote_addr,
            http.status_code = tracing::field::Empty,
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
        );

        // If we have a parent W3C context, attach it so the new span
        // continues the upstream trace. No-op when tracing is
        // disabled (`parent_cx` is `None`).
        if let Some(cx) = parent_cx {
            span.set_parent(cx);
        }

        let fut = self.service.call(req);
        let span_for_stamp = span.clone();

        Box::pin(
            async move {
                // `Instrument` makes the span the current span for
                // the entire duration of the inner future — every
                // `tracing::info!` and child span inside the handler
                // (e.g. the `http.upstream` span in `RouteHandler`)
                // becomes a child of this one.
                let mut res = fut.instrument(span).await?;

                // Re-enter the span so `stamp_response_headers` and
                // the OTel context both see the same trace/span IDs
                // when stamping the response.
                let _enter_guard = span_for_stamp.enter();
                let status = res.status();
                span_for_stamp.record("http.status_code", status.as_u16());
                if status.is_server_error() {
                    span_for_stamp.record("otel.status_code", "ERROR");
                } else {
                    span_for_stamp.record("otel.status_code", "OK");
                }
                stamp_response_headers(res.headers_mut());

                Ok(res)
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header::{HeaderName, HeaderValue};
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App, HttpResponse};

    async fn dummy_ok() -> HttpResponse {
        HttpResponse::Ok().body("hello")
    }

    async fn dummy_500() -> HttpResponse {
        HttpResponse::InternalServerError().body("oops")
    }

    #[actix_web::test]
    async fn middleware_is_noop_when_tracing_disabled() {
        // `init_telemetry` was never called, so `is_initialized()` is
        // false. The middleware should still let requests through
        // and must NOT add `x-trace-id` / `x-span-id` headers.
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .route("/ok", web::get().to(dummy_ok)),
        )
        .await;

        let req = test::TestRequest::get().uri("/ok").to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            res.headers().get("x-trace-id").is_none(),
            "x-trace-id must NOT be set when tracing is disabled"
        );
        assert!(
            res.headers().get("x-span-id").is_none(),
            "x-span-id must NOT be set when tracing is disabled"
        );
    }

    #[actix_web::test]
    async fn middleware_records_status_code_500() {
        // Even with tracing disabled, the middleware must pass the
        // request through unchanged — this is a smoke test for the
        // span `record` path (no panic when `is_initialized()` is
        // false).
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .route("/err", web::get().to(dummy_500)),
        )
        .await;

        let req = test::TestRequest::get().uri("/err").to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[actix_web::test]
    async fn middleware_forwards_custom_request_headers() {
        // Make sure the middleware doesn't strip headers (it should
        // only ever *add* `x-trace-id` / `x-span-id` when tracing is
        // enabled).
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .route("/probe", web::get().to(dummy_ok)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/probe")
            .insert_header(("x-custom", "value-1"))
            .to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        // The custom request header isn't echoed back (the dummy
        // handler doesn't do that), but the response should be a
        // 200 — proving the middleware didn't drop the request.
    }

    #[actix_web::test]
    async fn middleware_with_existing_traceparent_does_not_panic() {
        // Pass a syntactically-valid `traceparent` header. When
        // tracing is disabled, the extractor short-circuits, so this
        // is just a smoke test that the middleware handles real
        // W3C headers gracefully.
        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .route("/trace", web::get().to(dummy_ok)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/trace")
            .insert_header((
                HeaderName::from_static("traceparent"),
                HeaderValue::from_static(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                ),
            ))
            .to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), StatusCode::OK);
    }
}
