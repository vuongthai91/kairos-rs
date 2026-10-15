//! Integration tests for the admin WebSocket endpoint at `/ws/admin/metrics`.
//!
//! These tests spin up a real `actix_web::HttpServer` and connect to it
//! via `tokio_tungstenite`. We then assert on the snapshot JSON the
//! server pushes once per second.
//!
//! The tests deliberately do **not** exercise the full `kairos-gateway`
//! binary — they wire up just the WebSocket route + a `MetricsCollector`
//! so we can drive the metrics state from the test directly.
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use futures_util::{SinkExt, StreamExt};
use kairos_rs::routes::{
    metrics::MetricsCollector,
    websocket_admin::{configure_admin_websocket, AdminSnapshot},
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as TgMessage;

/// Helper: start an `actix_web::HttpServer` on a random port, return
/// the bound address and a join handle. The server only exposes the
/// admin WebSocket route.
async fn spawn_test_server(
    metrics: MetricsCollector,
) -> (String, tokio::task::JoinHandle<std::io::Result<()>>) {
    // Bind to port 0 to get a random free port.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local_addr");
    drop(listener);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(metrics.clone()))
            .configure(configure_admin_websocket)
    })
    .workers(1)
    .bind(("127.0.0.1", addr.port()))
    .expect("bind")
    .run();

    let handle = tokio::spawn(server);
    // Give the server a moment to actually start listening.
    tokio::time::sleep(Duration::from_millis(200)).await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_ws_upgrade_succeeds_and_sends_snapshot() {
    let metrics = MetricsCollector::default();
    metrics.record_request(true, Duration::from_millis(15), 200, None, None);
    metrics.record_request(false, Duration::from_millis(750), 500, None, None);
    metrics.record_route_request("api", true, Duration::from_millis(20), 200);

    let (addr, handle) = spawn_test_server(metrics).await;
    let url = format!("ws://{}/ws/admin/metrics", addr);

    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WebSocket upgrade must succeed");

    // The server pushes one snapshot per second. Wait up to 3 seconds
    // for the first one to arrive.
    let mut first_snapshot: Option<AdminSnapshot> = None;
    for _ in 0..30 {
        match tokio::time::timeout(Duration::from_millis(100), ws.next()).await {
            Ok(Some(Ok(TgMessage::Text(text)))) => {
                let snap: AdminSnapshot =
                    serde_json::from_str(&text).expect("snapshot must parse");
                first_snapshot = Some(snap);
                break;
            }
            Ok(Some(Ok(TgMessage::Ping(_)))) | Ok(Some(Ok(TgMessage::Pong(_)))) => continue,
            Ok(Some(Ok(TgMessage::Close(_)))) => break,
            Ok(Some(Err(_))) | Err(_) => continue,
            Ok(None) => break,
            _ => continue,
        }
    }

    let snap = first_snapshot.expect("server must push at least one snapshot");
    assert_eq!(snap.requests_total, 2);
    assert_eq!(snap.requests_success, 1);
    assert_eq!(snap.requests_error, 1);
    assert!(!snap.timestamp.is_empty());
    assert!(snap.latency_p50_ms <= 100.0, "p50 = {}", snap.latency_p50_ms);
    assert!(snap.latency_p95_ms >= 100.0, "p95 = {}", snap.latency_p95_ms);
    assert!(snap.latency_p99_ms >= 100.0, "p99 = {}", snap.latency_p99_ms);
    assert_eq!(snap.routes.len(), 1);
    assert_eq!(snap.routes[0].route_id, "api");
    assert_eq!(snap.routes[0].requests_total, 1);

    // Close gracefully.
    let _ = ws.send(TgMessage::Close(None)).await;
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_ws_handles_ping_with_pong() {
    let metrics = MetricsCollector::default();
    let (addr, handle) = spawn_test_server(metrics).await;
    let url = format!("ws://{}/ws/admin/metrics", addr);

    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WebSocket upgrade must succeed");

    // Send a ping; expect a pong back from the server.
    ws.send(TgMessage::Ping(b"keepalive".to_vec()))
        .await
        .expect("send ping");

    let mut got_pong = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_millis(100), ws.next()).await {
            Ok(Some(Ok(TgMessage::Pong(_)))) => {
                got_pong = true;
                break;
            }
            Ok(Some(Ok(TgMessage::Ping(_)))) | Ok(Some(Ok(TgMessage::Text(_)))) => continue,
            _ => continue,
        }
    }
    assert!(got_pong, "server must respond to client ping with pong");

    let _ = ws.send(TgMessage::Close(None)).await;
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_ws_multiple_clients_receive_independent_streams() {
    let metrics = MetricsCollector::default();
    metrics.record_request(true, Duration::from_millis(5), 200, None, None);
    let (addr, handle) = spawn_test_server(metrics).await;
    let url = format!("ws://{}/ws/admin/metrics", addr);

    let (mut ws1, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    async fn await_first_text<S>(ws: &mut S) -> AdminSnapshot
    where
        S: futures_util::StreamExt<Item = Result<TgMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        for _ in 0..30 {
            if let Ok(Some(Ok(TgMessage::Text(text)))) =
                tokio::time::timeout(Duration::from_millis(100), ws.next()).await
            {
                return serde_json::from_str(&text).expect("parse");
            }
        }
        panic!("never received a snapshot");
    }

    let s1 = await_first_text(&mut ws1).await;
    let s2 = await_first_text(&mut ws2).await;
    assert_eq!(s1.requests_total, 1);
    assert_eq!(s2.requests_total, 1);
    // Both clients should observe the same underlying metrics
    // regardless of when their first tick happened.
    assert_eq!(s1.latency_p50_ms, s2.latency_p50_ms);
    assert_eq!(s1.latency_p95_ms, s2.latency_p95_ms);
    assert_eq!(s1.latency_p99_ms, s2.latency_p99_ms);
    assert_eq!(s1.requests_per_second, s2.requests_per_second);

    let _ = ws1.send(TgMessage::Close(None)).await;
    let _ = ws2.send(TgMessage::Close(None)).await;
    handle.abort();
}
