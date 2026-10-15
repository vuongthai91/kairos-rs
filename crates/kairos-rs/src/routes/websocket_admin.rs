use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::interval;

use crate::routes::metrics::{MetricsCollector, RouteMetricsSnapshot};

/// Per-route summary sent inside the admin WebSocket snapshot.
///
/// This is a trimmed-down version of [`RouteMetricsSnapshot`] that only
/// carries the fields the admin dashboard actually needs, to keep the
/// push payload small.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRouteSummary {
    pub route_id: String,
    pub requests_total: u64,
    pub requests_success: u64,
    pub requests_error: u64,
    pub avg_response_time_ms: f64,
    pub success_rate: f64,
}

impl From<&RouteMetricsSnapshot> for AdminRouteSummary {
    fn from(s: &RouteMetricsSnapshot) -> Self {
        Self {
            route_id: s.route_id.clone(),
            requests_total: s.requests_total,
            requests_success: s.requests_success,
            requests_error: s.requests_error,
            avg_response_time_ms: s.avg_response_time_ms,
            success_rate: s.success_rate,
        }
    }
}

/// Snapshot pushed to the admin dashboard over `/ws/admin/metrics`.
///
/// Extends the basic counters with latency percentiles (p50/p95/p99),
/// throughput (requests/sec), and a per-route summary so the dashboard
/// doesn't have to combine the WebSocket feed with separate REST polls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSnapshot {
    pub timestamp: String,
    pub requests_total: u64,
    pub requests_success: u64,
    pub requests_error: u64,
    pub active_connections: u64,
    pub peak_connections: u64,
    pub uptime_seconds: u64,
    pub avg_response_time_ms: f64,
    pub success_rate: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub requests_per_second: f64,
    pub routes: Vec<AdminRouteSummary>,
}

/// Build a snapshot from the current state of the [`MetricsCollector`].
///
/// Extracted as a free function (instead of being inlined into the
/// WebSocket task body) so it can be exercised from unit tests
/// without spinning up an HTTP server.
pub fn build_admin_snapshot(metrics: &MetricsCollector) -> AdminSnapshot {
    let total_requests = metrics.requests_total.load(Ordering::Relaxed);
    let success_requests = metrics.requests_success.load(Ordering::Relaxed);
    let error_requests = metrics.requests_error.load(Ordering::Relaxed);
    let response_time_sum = metrics.response_time_sum.load(Ordering::Relaxed);
    let active_connections = metrics.active_connections.load(Ordering::Relaxed);
    let peak_connections = metrics.peak_connections.load(Ordering::Relaxed);
    let avg_response_time_ms = if total_requests > 0 {
        response_time_sum as f64 / total_requests as f64
    } else {
        0.0
    };
    let success_rate = if total_requests > 0 {
        (success_requests as f64 / total_requests as f64) * 100.0
    } else {
        100.0
    };
    let (latency_p50_ms, latency_p95_ms, latency_p99_ms) = metrics.latency_percentiles();
    let requests_per_second = metrics.requests_per_second();
    let routes: Vec<AdminRouteSummary> = metrics
        .route_metrics_snapshot()
        .iter()
        .map(AdminRouteSummary::from)
        .collect();
    AdminSnapshot {
        timestamp: chrono::Utc::now().to_rfc3339(),
        requests_total: total_requests,
        requests_success: success_requests,
        requests_error: error_requests,
        active_connections,
        peak_connections,
        uptime_seconds: metrics.start_time.elapsed().as_secs(),
        avg_response_time_ms,
        success_rate,
        latency_p50_ms,
        latency_p95_ms,
        latency_p99_ms,
        requests_per_second,
        routes,
    }
}

/// WebSocket handler for real-time admin metrics.
///
/// This endpoint establishes a WebSocket connection that pushes real-time
/// metrics updates to the connected client (typically the Admin UI).
///
/// # Protocol
///
/// - **Server -> Client**: JSON object containing current [`AdminSnapshot`]
///   pushed every second.
/// - **Client -> Server**: Ping/Pong for keepalive; `Close` to disconnect.
pub async fn admin_metrics_ws(
    req: HttpRequest,
    stream: web::Payload,
    metrics: web::Data<MetricsCollector>,
) -> Result<HttpResponse, Error> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    let metrics = metrics.into_inner();

    actix_web::rt::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let snapshot = build_admin_snapshot(&metrics);
                    let payload = match serde_json::to_string(&snapshot) {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    if session.text(payload).await.is_err() {
                        break;
                    }
                }

                msg = msg_stream.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            match msg {
                                Message::Ping(bytes) => {
                                    if session.pong(&bytes).await.is_err() { break; }
                                }
                                Message::Close(reason) => {
                                    let _ = session.close(reason).await;
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Some(Err(_)) | None => break,
                    }
                }
            }
        }
    });

    Ok(response)
}

/// Configures the admin WebSocket route.
pub fn configure_admin_websocket(cfg: &mut web::ServiceConfig) {
    cfg.route("/ws/admin/metrics", web::get().to(admin_metrics_ws));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_zero_when_no_requests_recorded() {
        let m = MetricsCollector::default();
        let snap = build_admin_snapshot(&m);
        assert_eq!(snap.requests_total, 0);
        assert_eq!(snap.requests_success, 0);
        assert_eq!(snap.requests_error, 0);
        assert_eq!(snap.latency_p50_ms, 0.0);
        assert_eq!(snap.latency_p95_ms, 0.0);
        assert_eq!(snap.latency_p99_ms, 0.0);
        assert_eq!(snap.requests_per_second, 0.0);
        assert_eq!(snap.success_rate, 100.0);
        assert_eq!(snap.avg_response_time_ms, 0.0);
        assert!(snap.routes.is_empty());
        assert!(!snap.timestamp.is_empty());
    }

    #[test]
    fn snapshot_reflects_recorded_requests_and_percentiles() {
        let m = MetricsCollector::default();
        for _ in 0..5 {
            m.record_request(true, Duration::from_millis(10), 200, None, None);
        }
        for _ in 0..3 {
            m.record_request(false, Duration::from_millis(800), 503, None, None);
        }
        let snap = build_admin_snapshot(&m);
        assert_eq!(snap.requests_total, 8);
        assert_eq!(snap.requests_success, 5);
        assert_eq!(snap.requests_error, 3);
        assert!(snap.latency_p50_ms <= 100.0);
        assert!(
            snap.latency_p95_ms >= 100.0,
            "p95_ms = {}, expected >= 100.0",
            snap.latency_p95_ms
        );
        assert!(
            snap.latency_p99_ms >= 100.0,
            "p99_ms = {}, expected >= 100.0",
            snap.latency_p99_ms
        );
        let expected_avg =
            (5 * 10 + 3 * 800) as f64 / 8.0;
        assert!((snap.avg_response_time_ms - expected_avg).abs() < 0.5);
        let expected_success_rate = 5.0 / 8.0 * 100.0;
        assert!((snap.success_rate - expected_success_rate).abs() < 0.01);
    }

    #[test]
    fn snapshot_serializes_to_valid_json_with_expected_keys() {
        let m = MetricsCollector::default();
        m.record_request(true, Duration::from_millis(5), 200, None, None);
        let snap = build_admin_snapshot(&m);
        let json = serde_json::to_string(&snap).expect("serialize");
        for key in [
            "timestamp",
            "requests_total",
            "requests_success",
            "requests_error",
            "active_connections",
            "peak_connections",
            "uptime_seconds",
            "avg_response_time_ms",
            "success_rate",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
            "requests_per_second",
            "routes",
        ] {
            assert!(
                json.contains(key),
                "expected key '{}' in serialized AdminSnapshot, got: {}",
                key,
                json
            );
        }
    }

    #[test]
    fn snapshot_routes_array_matches_route_metrics_snapshot() {
        let m = MetricsCollector::default();
        m.record_route_request("alpha", true, Duration::from_millis(20), 200);
        m.record_route_request("beta", true, Duration::from_millis(40), 200);
        m.record_route_request("alpha", false, Duration::from_millis(150), 500);
        let snap = build_admin_snapshot(&m);
        assert_eq!(snap.routes.len(), 2);
        let alpha = snap
            .routes
            .iter()
            .find(|r| r.route_id == "alpha")
            .expect("alpha route present");
        assert_eq!(alpha.requests_total, 2);
        assert_eq!(alpha.requests_success, 1);
        assert_eq!(alpha.requests_error, 1);
        let beta = snap
            .routes
            .iter()
            .find(|r| r.route_id == "beta")
            .expect("beta route present");
        assert_eq!(beta.requests_total, 1);
        assert_eq!(beta.requests_success, 1);
    }
}