//! OpenTelemetry telemetry status endpoint (PR8).
//!
//! Exposes a read-only view of the current telemetry configuration
//! + initialization state so the frontend `Telemetry.vue` panel
//! can show operators whether tracing is live, where spans are
//! being exported, and what sampling ratio is in effect.
//!
//! # Endpoints
//!
//! - `GET /api/telemetry` — returns the current status as JSON.
//!
//! # Why not under `/api/config/telemetry`?
//!
//! This endpoint reports **runtime** state (is the tracer provider
//! live right now?), not just the static config. The `/api/config`
//! family is reserved for the `config.json` view in the frontend;
//! mixing the two would force every read to parse the whole
//! settings struct. A dedicated path also makes it trivial to
//! lock down behind a future admin-auth middleware without
//! touching the rest of `/api/config`.

use actix_web::{web, HttpResponse, Result};
use serde::Serialize;

use crate::models::settings::Settings;
use crate::observability::tracing::is_initialized;

/// JSON shape returned by `GET /api/telemetry`. Kept stable for the
/// frontend — see `frontend/kairos-ui/src/types/telemetry.ts` (PR8).
#[derive(Debug, Serialize)]
pub struct TelemetryStatus {
    /// Master switch from `config.json > telemetry.enabled`.
    pub enabled: bool,
    /// OTLP/gRPC endpoint, `None` when telemetry is disabled or the
    /// operator didn't set one.
    pub otlp_endpoint: Option<String>,
    /// `service.name` resource attribute stamped on every span.
    pub service_name: String,
    /// Head-based sampler ratio (`0.0` = drop, `1.0` = always on).
    pub sample_ratio: f64,
    /// `true` when the global tracer provider is live and spans are
    /// being exported. `false` when `init_telemetry` was never
    /// called, returned `None` (e.g. disabled config, init
    /// failure), or the guard was dropped.
    pub initialized: bool,
}

/// Build a [`TelemetryStatus`] from a [`Settings`] snapshot.
///
/// Pulls values from `settings.telemetry`, falling back to
/// [`TelemetryConfig::default`] when the operator didn't set the
/// block in `config.json`. The `initialized` flag is read from
/// the observability module at call time — it's not stored in
/// the static config.
pub fn telemetry_status_from_settings(settings: &Settings) -> TelemetryStatus {
    let cfg = settings.telemetry.clone().unwrap_or_default();
    TelemetryStatus {
        enabled: cfg.enabled,
        otlp_endpoint: cfg.otlp_endpoint,
        service_name: cfg.service_name,
        sample_ratio: cfg.sample_ratio,
        initialized: is_initialized(),
    }
}

/// `GET /api/telemetry` handler. Returns the JSON status with
/// `200 OK`. This endpoint never errors — the worst case is an
/// empty config (`enabled: false`, `initialized: false`).
pub async fn get_telemetry_status(settings: web::Data<Settings>) -> Result<HttpResponse> {
    let status = telemetry_status_from_settings(settings.get_ref());
    Ok(HttpResponse::Ok().json(status))
}

/// Register the `/api/telemetry` route on the given `ServiceConfig`.
/// Call this from the gateway's `App::new()` block, e.g.:
///
/// ```ignore
/// App::new()
///     .configure(telemetry::configure_telemetry)
/// ```
pub fn configure_telemetry(cfg: &mut web::ServiceConfig) {
    cfg.route("/api/telemetry", web::get().to(get_telemetry_status));
}

#[cfg(test)]
mod tests {
    use super::*;
    // Renamed from `actix_web::test` because the bare `test`
    // name shadows the `#[test]` attribute macro from
    // `std::prelude`, which made the two sync tests below fail
    // to compile with "the async keyword is missing from the
    // function declaration" — the macro resolved to
    // `#[actix_web::test]` instead of `#[std::test]`.
    use actix_web::test as actix_test;
    use std::sync::Arc;
    use crate::observability::tracing::TelemetryConfig;

    /// Build a `Settings` with the given telemetry block. Most other
    /// fields are defaulted because the telemetry endpoint only
    /// reads `settings.telemetry` + the runtime `is_initialized()`.
    fn make_settings(telemetry: Option<TelemetryConfig>) -> Settings {
        Settings {
            version: 1,
            jwt: None,
            rate_limit: None,
            ai: None,
            telemetry,
            compression: None,
            routers: vec![],
        }
    }

    #[test]
    fn status_reflects_disabled_config() {
        let s = make_settings(None);
        let status = telemetry_status_from_settings(&s);
        assert!(!status.enabled);
        assert!(status.otlp_endpoint.is_none());
        assert_eq!(status.service_name, "kairos-gateway");
        assert!((status.sample_ratio - 1.0).abs() < 1e-9);
        // `initialized` is the runtime flag flipped by
        // `init_telemetry`. We don't assert its value here because
        // other tests in this binary (`observability::tracing::tests::
        // init_telemetry_lifecycle`) install and tear down the
        // global tracer provider; reading the flag from a
        // concurrent test is racy. The flag is covered by
        // `observability::tracing::tests::init_telemetry_*` and the
        // HTTP-level shape below.
    }

    #[test]
    fn status_reflects_enabled_config_with_endpoint() {
        let cfg = TelemetryConfig {
            enabled: true,
            service_name: "kairos-test".to_string(),
            otlp_endpoint: Some("http://collector:4317".to_string()),
            sample_ratio: 0.25,
        };
        let s = make_settings(Some(cfg));
        let status = telemetry_status_from_settings(&s);
        assert!(status.enabled);
        assert_eq!(status.otlp_endpoint.as_deref(), Some("http://collector:4317"));
        assert_eq!(status.service_name, "kairos-test");
        assert!((status.sample_ratio - 0.25).abs() < 1e-9);
    }

    #[actix_web::test]
    async fn get_telemetry_status_returns_200_and_json_shape() {
        let s = Arc::new(make_settings(None));
        let app = actix_test::init_service(
            actix_web::App::new()
                .app_data(web::Data::from(s))
                .configure(configure_telemetry),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/api/telemetry")
            .to_request();
        let res = actix_test::call_service(&app, req).await;
        assert_eq!(res.status().as_u16(), 200);
        assert_eq!(
            res.headers()
                .get(actix_web::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json"),
        );
        let body: serde_json::Value = actix_test::read_body_json(res).await;
        assert_eq!(body["enabled"], false);
        assert!(body["otlp_endpoint"].is_null());
        assert_eq!(body["service_name"], "kairos-gateway");
        assert!((body["sample_ratio"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        // `initialized` is the runtime flag set by
        // `init_telemetry` — see the note in
        // `status_reflects_disabled_config` for why we don't assert
        // it here (other tests in the same binary race on the
        // process-global flag).
    }

    #[actix_web::test]
    async fn get_telemetry_status_404_for_wrong_path() {
        let s = Arc::new(make_settings(None));
        let app = actix_test::init_service(
            actix_web::App::new()
                .app_data(web::Data::from(s))
                .configure(configure_telemetry),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/api/telemetry/extra")
            .to_request();
        let res = actix_test::call_service(&app, req).await;
        assert_eq!(res.status().as_u16(), 404);
    }
}
