//! End-to-end integration tests for the WebSocket permessage-deflate
//! proxy.
//!
//! Strategy: spin up a minimal WebSocket server in the test that
//! *also* speaks permessage-deflate on its side, then drive the
//! gateway through `tokio_tungstenite` with the
//! `Sec-WebSocket-Extensions: permessage-deflate` header set, and
//! verify that:
//!
//! 1. The gateway responds with the negotiated extension.
//! 2. Bytes the client sends are compressed in transit to the
//!    backend (we check by inspecting the gateway's behavior).
//! 3. Bytes the backend sends are decompressed in transit to the
//!    client (so the client receives the original plaintext).
//!
//! The proxy is wired into the actix `HttpServer` via
//! `WebSocketHandler::handle_websocket` directly — we don't go
//! through the full route-matching machinery. The test constructs
//! a `Backend`, calls `handle_websocket`, and asserts that the
//! bytes the client sends and receives round-trip correctly.

use actix_web::{web, App, HttpServer};
use futures_util::{SinkExt, StreamExt};
use kairos_rs::models::router::Backend;
use kairos_rs::services::websocket::WebSocketHandler;
use kairos_rs::services::ws_compression::{
    CompressionConfig, DecompressedContext, DeflateContext, RouterCompressionConfig,
    PERMESSAGE_DEFLATE,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message as WsMsg;
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::client::IntoClientRequest,
};

async fn pick_free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap()
}

/// A backend WebSocket server that speaks permessage-deflate on
/// its side. It applies per-message deflate / inflate (the same
/// code the gateway uses internally) so the round trip works
/// regardless of whether the gateway is configured to compress.
async fn run_compression_backend(
    addr: SocketAddr,
    negotiated_to_advertise: String,
) {
    let listener = TcpListener::bind(addr).await.expect("bind backend");
    let (stream, _) = listener.accept().await.expect("accept backend");
    let callback = |req: &Request, response: Response| {
        // Build a response that *copies* the headers tungstenite
        // already produced (so we keep Upgrade: websocket, etc.)
        // and just adds our extension advertisement.
        let mut builder = Response::builder().status(response.status());
        for (k, v) in response.headers().iter() {
            if k.as_str().eq_ignore_ascii_case("sec-websocket-extensions") {
                continue;
            }
            builder = builder.header(k.as_str(), v.as_bytes());
        }
        builder = builder.header("Sec-WebSocket-Extensions", &negotiated_to_advertise);
        let response = builder.body(()).unwrap();
        Ok(response)
    };
    let mut ws = accept_hdr_async(stream, callback)
        .await
        .expect("backend handshake");
    let deflater = DeflateContext::new();
    let inflater = DecompressedContext::new();
    while let Some(msg) = ws.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            WsMsg::Binary(b) => {
                // Backend expects compressed bytes (it advertised permessage-deflate).
                let decompressed = inflater.decompress_message(&b).expect("backend inflate");
                let recompressed = deflater.compress_message(&decompressed.0).0;
                if ws.send(WsMsg::Binary(recompressed)).await.is_err() {
                    break;
                }
            }
            WsMsg::Close(_) => break,
            _ => continue,
        }
    }
}

/// A backend that does NOT speak permessage-deflate. Just echoes
/// raw bytes back unchanged.
async fn run_plain_echo_backend(addr: SocketAddr) {
    let listener = TcpListener::bind(addr).await.expect("bind plain");
    let (stream, _) = listener.accept().await.expect("accept plain");
    let mut ws = accept_hdr_async(stream, |_req: &Request, resp: Response| Ok(resp))
        .await
        .expect("plain handshake");
    while let Some(msg) = ws.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            WsMsg::Text(t) => {
                if ws.send(WsMsg::Text(t)).await.is_err() {
                    break;
                }
            }
            WsMsg::Binary(b) => {
                if ws.send(WsMsg::Binary(b)).await.is_err() {
                    break;
                }
            }
            WsMsg::Close(_) => break,
            _ => continue,
        }
    }
}

#[tokio::test]
async fn compression_negotiated_and_text_round_trips() {
    let backend_addr = pick_free_port().await;
    tokio::spawn(run_compression_backend(
        backend_addr,
        PERMESSAGE_DEFLATE.to_string(),
    ));

    let compression = CompressionConfig {
        enabled: true,
        server_no_context_takeover: false,
        client_no_context_takeover: false,
        max_window_bits: None,
    };
    let ws_handler = WebSocketHandler::new(30);

    // Build a real actix `HttpServer`, hit it via tokio_tungstenite,
    // and verify the round trip.
    let ws_data = web::Data::new(ws_handler);
    let backend_clone = backend_addr;
    let compression_clone = compression;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway_addr: SocketAddr = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(ws_data.clone())
            .app_data(web::Data::new(backend_clone))
            .app_data(web::Data::new(compression_clone.clone()))
            .route("/ws", web::get().to(test_ws_no_compression_offered))
    })
    .workers(1)
    .listen(listener)
    .unwrap();
    tokio::spawn(server.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Client: connect with permessage-deflate offer.
    let url = format!("ws://{}/ws", gateway_addr);
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Extensions",
        PERMESSAGE_DEFLATE.parse().unwrap(),
    );
    let (mut ws, response) = connect_async(request).await.expect("client connect");

    let response_ext = response
        .headers()
        .get("Sec-WebSocket-Extensions")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        response_ext.contains(PERMESSAGE_DEFLATE),
        "gateway should advertise permessage-deflate in upgrade response, got: {:?}",
        response_ext
    );

    // Send a long-ish text payload that benefits from compression.
    let payload = "the quick brown fox jumps over the lazy dog. ".repeat(20);
    ws.send(WsMsg::Text(payload.clone())).await.unwrap();

    // The gateway should compress → backend inflates → backend
    // re-compresses → gateway inflates → client receives. We
    // sent a Text, but the gateway converts to Binary before
    // sending (because compressed permessage-deflate frames
    // are always Binary), so the backend will only see Binary.
    // The response will also be Binary.
    let received = ws.next().await.expect("response").expect("response ok");
    let received_bytes = match received {
        WsMsg::Binary(b) => b,
        WsMsg::Text(t) => t.into_bytes(),
        other => panic!(
            "unexpected frame type, got: {:?}",
            std::mem::discriminant(&other)
        ),
    };
    let received_str = String::from_utf8(received_bytes).expect("utf8");
    assert_eq!(
        received_str, payload,
        "text echo mismatch: original_len={} received_len={}",
        payload.len(),
        received_str.len()
    );

    ws.close(None).await.ok();
}

#[tokio::test]
async fn no_compression_when_disabled() {
    let backend_addr = pick_free_port().await;
    tokio::spawn(run_plain_echo_backend(backend_addr));

    let ws_handler = WebSocketHandler::new(30);
    use actix_web::{web, App, HttpServer};
    let ws_data = web::Data::new(ws_handler);
    let backend_clone = backend_addr;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway_addr: SocketAddr = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(ws_data.clone())
            .app_data(web::Data::new(backend_clone))
            .route("/ws", web::get().to(test_ws_no_compression))
    })
    .workers(1)
    .listen(listener)
    .unwrap();
    tokio::spawn(server.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let url = format!("ws://{}/ws", gateway_addr);
    let (mut ws, _response) = connect_async(url).await.expect("plain connect");

    let payload = "plain text, no compression";
    ws.send(WsMsg::Text(payload.to_string())).await.unwrap();
    let received = ws.next().await.expect("echo").expect("echo ok");
    let received_text = match received {
        WsMsg::Text(t) => t,
        other => panic!("expected text, got: {:?}", std::mem::discriminant(&other)),
    };
    assert_eq!(received_text, payload);

    ws.close(None).await.ok();
}

async fn test_ws_no_compression(
    req: actix_web::HttpRequest,
    stream: web::Payload,
    ws_handler: web::Data<WebSocketHandler>,
    backend_addr: web::Data<SocketAddr>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let backend = Backend {
        host: format!("http://{}", backend_addr.ip()),
        port: backend_addr.port(),
        weight: 1,
        health_check_path: None,
        tls: None,
    };
    ws_handler
        .handle_websocket(req, stream, &backend, "/", None)
        .await
}

#[tokio::test]
async fn no_compression_when_client_doesnt_offer() {
    let backend_addr = pick_free_port().await;
    tokio::spawn(run_plain_echo_backend(backend_addr));

    let ws_handler = WebSocketHandler::new(30);
    let compression = CompressionConfig {
        enabled: true,
        ..Default::default()
    };
    use actix_web::{web, App, HttpServer};
    let ws_data = web::Data::new(ws_handler);
    let backend_clone = backend_addr;
    let compression_clone = compression;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway_addr: SocketAddr = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(ws_data.clone())
            .app_data(web::Data::new(backend_clone))
            .app_data(web::Data::new(compression_clone.clone()))
            .route("/ws", web::get().to(test_ws_no_compression_offered))
    })
    .workers(1)
    .listen(listener)
    .unwrap();
    tokio::spawn(server.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let url = format!("ws://{}/ws", gateway_addr);
    // Client does NOT offer permessage-deflate.
    let (mut ws, response) = connect_async(url).await.expect("plain connect");
    let response_ext = response
        .headers()
        .get("Sec-WebSocket-Extensions")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        !response_ext.contains(PERMESSAGE_DEFLATE),
        "gateway must not advertise permessage-deflate when client didn't offer, got: {:?}",
        response_ext
    );

    let payload = "client without deflate offer";
    ws.send(WsMsg::Text(payload.to_string())).await.unwrap();
    let received = ws.next().await.expect("echo").expect("echo ok");
    let received_text = match received {
        WsMsg::Text(t) => t,
        other => panic!("expected text, got: {:?}", std::mem::discriminant(&other)),
    };
    assert_eq!(received_text, payload);

    ws.close(None).await.ok();
}

async fn test_ws_no_compression_offered(
    req: actix_web::HttpRequest,
    stream: web::Payload,
    ws_handler: web::Data<WebSocketHandler>,
    backend_addr: web::Data<SocketAddr>,
    compression: web::Data<CompressionConfig>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let backend = Backend {
        host: format!("http://{}", backend_addr.ip()),
        port: backend_addr.port(),
        weight: 1,
        health_check_path: None,
        tls: None,
    };
    ws_handler
        .handle_websocket(req, stream, &backend, "/", Some(&compression))
        .await
}

#[tokio::test]
async fn compression_with_no_context_takeover_round_trips() {
    let backend_addr = pick_free_port().await;
    tokio::spawn(run_compression_backend(
        backend_addr,
        PERMESSAGE_DEFLATE.to_string(),
    ));

    let compression = CompressionConfig {
        enabled: true,
        server_no_context_takeover: true,
        client_no_context_takeover: true,
        max_window_bits: None,
    };
    let ws_handler = WebSocketHandler::new(30);
    use actix_web::{web, App, HttpServer};
    let ws_data = web::Data::new(ws_handler);
    let backend_clone = backend_addr;
    let compression_clone = compression;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway_addr: SocketAddr = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(ws_data.clone())
            .app_data(web::Data::new(backend_clone))
            .app_data(web::Data::new(compression_clone.clone()))
            .route("/ws", web::get().to(test_ws_no_compression_offered))
    })
    .workers(1)
    .listen(listener)
    .unwrap();
    tokio::spawn(server.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let url = format!("ws://{}/ws", gateway_addr);
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Extensions",
        "permessage-deflate; client_no_context_takeover; server_no_context_takeover"
            .parse()
            .unwrap(),
    );
    let (mut ws, _response) = connect_async(request).await.expect("client connect");

    // Send two distinct payloads and verify each round-trips
    // (this catches bugs where a context-reuse-style implementation
    // would mix state across messages).
    let p1: Vec<u8> = b"first message".repeat(40);
    let p2: Vec<u8> = b"second message".repeat(40);
    ws.send(WsMsg::Binary(p1.clone())).await.unwrap();
    let r1 = ws.next().await.expect("r1").expect("r1 ok");
    let r1_bytes = match r1 {
        WsMsg::Binary(b) => b,
        WsMsg::Text(t) => t.into_bytes(),
        other => panic!("unexpected frame: {:?}", std::mem::discriminant(&other)),
    };
    assert_eq!(r1_bytes, p1, "first message round-trip mismatch");

    ws.send(WsMsg::Binary(p2.clone())).await.unwrap();
    let r2 = ws.next().await.expect("r2").expect("r2 ok");
    let r2_bytes = match r2 {
        WsMsg::Binary(b) => b,
        WsMsg::Text(t) => t.into_bytes(),
        other => panic!("unexpected frame: {:?}", std::mem::discriminant(&other)),
    };
    assert_eq!(r2_bytes, p2, "second message round-trip mismatch");

    ws.close(None).await.ok();
}

#[test]
fn negotiation_returns_none_when_disabled() {
    let cfg = CompressionConfig {
        enabled: false,
        ..Default::default()
    };
    assert!(cfg.negotiate("permessage-deflate").is_none());
}

#[test]
fn negotiation_returns_none_when_peer_does_not_offer() {
    let cfg = CompressionConfig {
        enabled: true,
        ..Default::default()
    };
    assert!(cfg.negotiate("").is_none());
    assert!(cfg.negotiate("x-foo; bar=1").is_none());
}

#[test]
fn negotiation_returns_some_when_peer_offers() {
    let cfg = CompressionConfig {
        enabled: true,
        ..Default::default()
    };
    let neg = cfg.negotiate("permessage-deflate").expect("negotiate");
    assert!(neg.to_header_value().contains(PERMESSAGE_DEFLATE));
}

#[test]
fn router_compression_config_validate_rejects_bad_window_bits() {
    let cfg = RouterCompressionConfig {
        max_window_bits: Some(7),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());

    let cfg = RouterCompressionConfig {
        max_window_bits: Some(16),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());

    let cfg = RouterCompressionConfig {
        max_window_bits: Some(8),
        ..Default::default()
    };
    assert!(cfg.validate().is_ok());
}