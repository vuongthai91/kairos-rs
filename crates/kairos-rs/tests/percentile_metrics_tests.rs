//! Integration tests for the latency percentile and throughput
//! metrics exposed by the Prometheus `/metrics` endpoint.
use actix_web::{test, web, App};
use kairos_rs::routes::metrics::{configure_metrics, MetricsCollector};
use std::time::Duration;

#[actix_web::test]
async fn test_prometheus_exposition_includes_latency_percentiles() {
    let m = MetricsCollector::default();
    for _ in 0..10 {
        m.record_request(true, Duration::from_millis(10), 200, None, None);
    }
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(m))
            .configure(configure_metrics),
    )
    .await;
    let req = test::TestRequest::get().uri("/metrics").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body = test::read_body(resp).await;
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(
        body_str.contains("# HELP kairos_response_time_p50"),
        "missing HELP for kairos_response_time_p50"
    );
    assert!(
        body_str.contains("# TYPE kairos_response_time_p50 gauge"),
        "kairos_response_time_p50 must be a gauge"
    );
    assert!(
        body_str.contains("# HELP kairos_response_time_p95"),
        "missing HELP for kairos_response_time_p95"
    );
    assert!(
        body_str.contains("# HELP kairos_response_time_p99"),
        "missing HELP for kairos_response_time_p99"
    );
}

#[actix_web::test]
async fn test_prometheus_exposition_includes_throughput() {
    let m = MetricsCollector::default();
    m.record_request(true, Duration::from_millis(1), 200, None, None);
    m.record_request(true, Duration::from_millis(1), 200, None, None);
    m.record_request(true, Duration::from_millis(1), 200, None, None);
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(m))
            .configure(configure_metrics),
    )
    .await;
    let req = test::TestRequest::get().uri("/metrics").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body = test::read_body(resp).await;
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(
        body_str.contains("# HELP kairos_requests_per_second"),
        "missing HELP for kairos_requests_per_second"
    );
    assert!(
        body_str.contains("# TYPE kairos_requests_per_second gauge"),
        "kairos_requests_per_second must be a gauge"
    );
    assert!(
        body_str.contains("kairos_requests_per_second"),
        "kairos_requests_per_second sample line missing"
    );
}
