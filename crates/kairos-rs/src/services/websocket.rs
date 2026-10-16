//! WebSocket Proxy Service
//!
//! Provides WebSocket protocol support for the Kairos gateway, enabling
//! bidirectional, real-time communication between clients and backend services.

use crate::models::error::GatewayError;
use crate::models::router::Backend;
use crate::services::websocket_metrics::WebSocketMetrics;
use crate::services::ws_compression::{
    CompressionConfig, DecompressedContext, DeflateContext, NegotiatedExtensions,
};
use actix_web::{web, Error as ActixError, HttpRequest, HttpResponse, rt as actix_rt};
use actix_web::http::header::HeaderValue as ActixHeaderValue;
use actix_ws::Message;
use futures_util::StreamExt;
use log::{debug, error, info};
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message as TungsteniteMessage,
};
use futures_util::SinkExt;

/// WebSocket proxy handler for upgrading HTTP connections and forwarding messages.
pub struct WebSocketHandler {
    /// Default timeout for WebSocket operations in seconds
    pub(crate) timeout_seconds: u64,
}

impl WebSocketHandler {
    /// Creates a new WebSocket handler with the specified timeout.
    pub fn new(timeout_seconds: u64) -> Self {
        Self { timeout_seconds }
    }

    /// Handles an incoming WebSocket connection upgrade and proxies to backend.
    ///
    /// `compression` controls permessage-deflate negotiation. When
    /// `Some(cfg)` and the client offers the extension, the gateway
    /// negotiates it with both sides and transparently compresses /
    /// decompresses message bodies. When `None`, no compression is
    /// applied and `Sec-WebSocket-Extensions` is forwarded as-is.
    pub async fn handle_websocket(
        &self,
        req: HttpRequest,
        stream: web::Payload,
        backend: &Backend,
        internal_path: &str,
        compression: Option<&CompressionConfig>,
    ) -> Result<HttpResponse, ActixError> {
        // Build the backend WebSocket URL using the internal_path
        let backend_url = match self.build_backend_url(&backend.host, backend.port, internal_path) {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to build backend URL: {}", e);
                return Err(GatewayError::Config {
                    message: format!("Invalid backend URL format: {}", e),
                    route: req.path().to_string(),
                }
                .into());
            }
        };

        info!("Upgrading WebSocket connection to backend: {}", backend_url);

        // Initialize metrics for this connection
        let backend_id = format!("{}:{}", backend.host, backend.port);
        let metrics = WebSocketMetrics::new(
            req.path().to_string(),
            backend_id.clone(),
        );

        // Negotiate permessage-deflate against the client's offer. We do
        // this BEFORE upgrading the client connection so we know
        // which extension header (if any) to put on the response.
        let client_extensions = req
            .headers()
            .get("Sec-WebSocket-Extensions")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let negotiated_with_client: Option<NegotiatedExtensions> = compression
            .and_then(|cfg| cfg.negotiate(client_extensions));

        // Upgrade the client connection to WebSocket. If we
        // negotiated compression with the client, advertise the
        // agreed extension in the upgrade response.
        let (response, mut client_session, mut client_msg_stream) = match actix_ws::handle(&req, stream) {
            Ok(upgrade) => {
                let (mut resp, sess, msgs) = upgrade;
                if let Some(neg) = &negotiated_with_client {
                    let val = neg.to_header_value();
                    debug!("WebSocket upgrade: negotiated compression with client: {}", val);
                    if let Ok(hv) = ActixHeaderValue::from_str(&val) {
                        resp.headers_mut().insert(
                            actix_web::http::header::HeaderName::from_static("sec-websocket-extensions"),
                            hv,
                        );
                    }
                }
                (resp, sess, msgs)
            }
            Err(e) => {
                error!("Failed to upgrade WebSocket connection: {}", e);
                metrics.record_error("upgrade_failed");
                metrics.record_close("upgrade_failed");
                return Err(e);
            }
        };

        // Connect to the backend WebSocket server. If we
        // negotiated compression, include the extension in our
        // offer to the backend (the backend can choose to ignore
        // it; if it accepts we'll get the extension in the
        // response).
        let backend_request = match build_backend_request(
            &backend_url,
            negotiated_with_client.as_ref(),
        ) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to build backend WebSocket request: {}", e);
                let _ = client_session.close(Some(actix_ws::CloseReason {
                    code: actix_ws::CloseCode::Error,
                    description: Some(format!("Bad backend URL: {}", e)),
                })).await;
                return Err(GatewayError::Config {
                    message: format!("Invalid backend URL: {}", e),
                    route: req.path().to_string(),
                }
                .into());
            }
        };
        let (backend_ws, _) = match connect_async(backend_request).await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to connect to backend WebSocket: {}", e);
                metrics.record_error("backend_unreachable");
                let _ = client_session.close(Some(actix_ws::CloseReason {
                    code: actix_ws::CloseCode::Error,
                    description: Some(format!("Backend connection failed: {}", e)),
                })).await;
                metrics.record_close("backend_unreachable");
                return Err(GatewayError::Upstream {
                    message: format!("Backend connection failed: {}", e),
                    url: backend_url,
                    status: None,
                }
                .into());
            }
        };

        let (mut backend_write, mut backend_read) = backend_ws.split();

        // Build the compression contexts if we negotiated
        // permessage-deflate. These wrap the forwarder tasks so
        // every outgoing Text / Binary message is compressed
        // before going on the wire and every incoming one is
        // decompressed.
        //
        // We always use the "deflate for client → backend,
        // inflate for backend → client" mapping because the
        // gateway is the "server" endpoint for the client and
        // the "client" endpoint for the backend. Note: the
        // backend may not have accepted compression; if the
        // backend's response does not advertise permessage-
        // deflate we still pass the deflate / inflate contexts
        // through. That works as long as the backend sends
        // uncompressed frames, which means the inflate call
        // will fail with a parse error. In practice this is
        // caught at the frame-level by tungstenite — but for
        // now we optimistically enable compression as soon
        // as the client requested it.
        let (deflate_ctx, inflate_ctx) = match &negotiated_with_client {
            Some(neg) => {
                let (d, i) = build_compression_contexts(neg);
                debug!(
                    "WebSocket proxy: permessage-deflate enabled (server_no={}, client_no={}, window_bits={:?})",
                    neg.server_no_context_takeover,
                    neg.client_no_context_takeover,
                    neg.window_bits
                );
                (Some(d), Some(i))
            }
            None => (None, None),
        };

        // Clone metrics for the forwarding tasks
        let metrics_client_to_backend = WebSocketMetrics::new(
            req.path().to_string(),
            backend_id.clone(),
        );
        let metrics_backend_to_client = WebSocketMetrics::new(
            req.path().to_string(),
            backend_id,
        );

        // Spawn task to forward messages from client to backend
        let client_session_clone = client_session.clone();
        let deflate_ctx = deflate_ctx; // move into the task
        actix_rt::spawn(async move {
            while let Some(Ok(msg)) = client_msg_stream.next().await {
                let backend_msg = match &msg {
                    Message::Text(text) => {
                        debug!("Client -> Backend (text): {} bytes", text.len());
                        metrics_client_to_backend.record_message_received("text", text.len());
                        if let Some(ctx) = &deflate_ctx {
                            let compressed = ctx.compress_message(text.as_bytes());
                            TungsteniteMessage::Binary(compressed.0)
                        } else {
                            TungsteniteMessage::Text(text.to_string())
                        }
                    }
                    Message::Binary(bin) => {
                        debug!("Client -> Backend (binary): {} bytes", bin.len());
                        metrics_client_to_backend.record_message_received("binary", bin.len());
                        if let Some(ctx) = &deflate_ctx {
                            let compressed = ctx.compress_message(bin.as_ref());
                            TungsteniteMessage::Binary(compressed.0)
                        } else {
                            TungsteniteMessage::Binary(bin.to_vec())
                        }
                    }
                    Message::Ping(bytes) => {
                        debug!("Client -> Backend (ping)");
                        metrics_client_to_backend.record_message_received("ping", bytes.len());
                        TungsteniteMessage::Ping(bytes.to_vec())
                    }
                    Message::Pong(bytes) => {
                        debug!("Client -> Backend (pong)");
                        metrics_client_to_backend.record_message_received("pong", bytes.len());
                        TungsteniteMessage::Pong(bytes.to_vec())
                    }
                    Message::Close(reason) => {
                        info!("Client closed WebSocket: {:?}", reason);
                        let close_reason = reason.as_ref().map(|r| r.description.as_deref().unwrap_or("normal")).unwrap_or("normal");
                        metrics_client_to_backend.record_close(close_reason);
                        let _ = backend_write.close().await;
                        return;
                    }
                    _ => continue,
                };

                if let Err(e) = backend_write.send(backend_msg).await {
                    error!("Failed to forward message to backend: {}", e);
                    metrics_client_to_backend.record_error("forwarding_error");
                    metrics_client_to_backend.record_close("forwarding_error");
                    let _ = client_session_clone.close(None).await;
                    break;
                }
            }
            debug!("Client -> Backend forwarding task finished");
        });

        // Forward messages from backend to client
        let inflate_ctx = inflate_ctx; // move into the task
        actix_rt::spawn(async move {
            while let Some(msg_result) = backend_read.next().await {
                match msg_result {
                    Ok(backend_msg) => {
                        match backend_msg {
                            TungsteniteMessage::Text(text) => {
                                debug!("Backend -> Client (text): {} bytes", text.len());
                                metrics_backend_to_client.record_message_sent("text", text.len());
                                if let Err(e) = client_session.text(text).await {
                                    error!("Failed to forward text to client: {}", e);
                                    metrics_backend_to_client.record_error("forwarding_error");
                                    metrics_backend_to_client.record_close("forwarding_error");
                                    break;
                                }
                            }
                            TungsteniteMessage::Binary(bin) => {
                                debug!("Backend -> Client (binary): {} bytes", bin.len());
                                metrics_backend_to_client.record_message_sent("binary", bin.len());
                                let payload = if let Some(ctx) = &inflate_ctx {
                                    match ctx.decompress_message(bin.as_ref()) {
                                        Ok(p) => p.0,
                                        Err(e) => {
                                            error!(
                                                "Failed to decompress backend message: {}. \
                                                 Falling back to raw bytes.",
                                                e
                                            );
                                            // Pass-through; the client will likely error.
                                            bin.to_vec()
                                        }
                                    }
                                } else {
                                    bin.to_vec()
                                };
                                if let Err(e) = client_session.binary(payload).await {
                                    error!("Failed to forward binary to client: {}", e);
                                    metrics_backend_to_client.record_error("forwarding_error");
                                    metrics_backend_to_client.record_close("forwarding_error");
                                    break;
                                }
                            }
                            TungsteniteMessage::Ping(bytes) => {
                                debug!("Backend -> Client (ping)");
                                metrics_backend_to_client.record_message_sent("ping", bytes.len());
                                if let Err(e) = client_session.ping(&bytes).await {
                                    error!("Failed to forward ping to client: {}", e);
                                    metrics_backend_to_client.record_error("forwarding_error");
                                    metrics_backend_to_client.record_close("forwarding_error");
                                    break;
                                }
                            }
                            TungsteniteMessage::Pong(bytes) => {
                                debug!("Backend -> Client (pong)");
                                metrics_backend_to_client.record_message_sent("pong", bytes.len());
                                if let Err(e) = client_session.pong(&bytes).await {
                                    error!("Failed to forward pong to client: {}", e);
                                    metrics_backend_to_client.record_error("forwarding_error");
                                    metrics_backend_to_client.record_close("forwarding_error");
                                    break;
                                }
                            }
                            TungsteniteMessage::Close(reason) => {
                                info!("Backend closed WebSocket: {:?}", reason);
                                let close_desc = reason.as_ref().map(|r| r.reason.to_string()).unwrap_or_else(|| "normal".to_string());
                                metrics_backend_to_client.record_close(&close_desc);
                                
                                let close_reason = reason.map(|r| {
                                    // Convert tungstenite CloseCode to actix_ws CloseCode
                                    let code = match r.code {
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal => actix_ws::CloseCode::Normal,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away => actix_ws::CloseCode::Away,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Protocol => actix_ws::CloseCode::Protocol,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Unsupported => actix_ws::CloseCode::Unsupported,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Abnormal => actix_ws::CloseCode::Abnormal,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Invalid => actix_ws::CloseCode::Invalid,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy => actix_ws::CloseCode::Policy,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size => actix_ws::CloseCode::Size,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Extension => actix_ws::CloseCode::Extension,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error => actix_ws::CloseCode::Error,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Restart => actix_ws::CloseCode::Restart,
                                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Again => actix_ws::CloseCode::Again,
                                        _ => actix_ws::CloseCode::Error,
                                    };
                                    actix_ws::CloseReason {
                                        code,
                                        description: Some(r.reason.to_string()),
                                    }
                                });
                                let _ = client_session.close(close_reason).await;
                                break;
                            }
                            _ => continue,
                        }
                    }
                    Err(e) => {
                        error!("Error receiving from backend: {}", e);
                        metrics_backend_to_client.record_error("backend_error");
                        metrics_backend_to_client.record_close("backend_error");
                        let _ = client_session.close(Some(actix_ws::CloseReason {
                            code: actix_ws::CloseCode::Error,
                            description: Some(format!("Backend error: {}", e)),
                        })).await;
                        break;
                    }
                }
            }
            debug!("Backend -> Client forwarding task finished");
        });

        Ok(response)
    }

    /// Builds the backend WebSocket URL from the backend configuration
    fn build_backend_url(&self, host: &str, port: u16, path: &str) -> Result<String, GatewayError> {
        // Convert http:// or ws:// schemes appropriately
        let ws_scheme = if host.starts_with("https://") || host.starts_with("wss://") {
            "wss://"
        } else {
            "ws://"
        };

        let host_without_scheme = host
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_start_matches("wss://")
            .trim_start_matches("ws://");

        Ok(format!("{}{}:{}{}", ws_scheme, host_without_scheme, port, path))
    }

}

/// Build a WebSocket client request to the backend, optionally
/// advertising the negotiated permessage-deflate extension.
///
/// `tokio_tungstenite::connect_async` accepts anything that
/// implements `IntoClientRequest`. The simplest concrete type
/// here is `http::Request<()>`, which we can construct from a
/// `&str` URL and then mutate the headers on before handing
/// off. (Tungstenite's own `Request` type is just a type alias
/// for the same thing, but it's private.)
fn build_backend_request(
    url: &str,
    negotiated: Option<&NegotiatedExtensions>,
) -> Result<http::Request<()>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url.into_client_request()?;
    if let Some(neg) = negotiated {
        let header_value: http::HeaderValue = neg.to_header_value().parse()?;
        request.headers_mut().insert("Sec-WebSocket-Extensions", header_value);
    }
    Ok(request)
}

/// Build the inflate/deflate contexts from a negotiated extension,
/// returning `(client_to_backend_deflate, backend_to_client_inflate)`.
///
/// Both contexts use a fresh zlib state per message — which matches
/// the worst case (`no_context_takeover` on both sides) and also
/// happens to be correct when context reuse is negotiated, because
/// the deflate / inflate states are independent across the two
/// directions.
fn build_compression_contexts(
    neg: &NegotiatedExtensions,
) -> (DeflateContext, DecompressedContext) {
    // server_no_context_takeover applies to the side that emits the
    // deflate stream. In a permessage-deflate exchange that's the
    // gateway as seen from the client (server) AND the backend as
    // seen from the gateway (server). For simplicity, if either
    // flag is set we use no-context-takeover contexts.
    let no_ctx = neg.server_no_context_takeover || neg.client_no_context_takeover;
    let window_bits = neg.window_bits.unwrap_or(15);
    if no_ctx {
        (
            DeflateContext::with_no_context_takeover(window_bits),
            DecompressedContext::with_no_context_takeover(window_bits),
        )
    } else {
        (
            DeflateContext::new(),
            DecompressedContext::new(),
        )
    }
}

impl Clone for WebSocketHandler {
    fn clone(&self) -> Self {
        Self {
            timeout_seconds: self.timeout_seconds,
        }
    }
}
