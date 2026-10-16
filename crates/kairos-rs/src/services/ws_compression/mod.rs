//! WebSocket permessage-deflate compression (RFC 7692).
//!
//! The gateway acts as a transparent proxy for WebSocket connections,
//! and we need to honor the `Sec-WebSocket-Extensions` header on both
//! sides. When both client and backend offer `permessage-deflate` and
//! the gateway is configured to negotiate it, the gateway:
//!
//! 1. Strips `permessage-deflate` from the client's offer before
//!    forwarding to the backend (the gateway is the endpoint as far
//!    as the backend is concerned).
//! 2. Compresses client→backend messages before forwarding.
//! 3. Decompresses backend→client messages before forwarding.
//! 4. Adds the negotiated extension to the backend's response so
//!    the backend knows the channel is compressed.
//!
//! The `tungstenite-0.21` crate only parses the
//! `Sec-WebSocket-Extensions` header; it does not implement the
//! compression itself. This module provides both pieces:
//!
//! - [`extensions`] — header parsing + parameter negotiation
//! - [`deflate`] — per-message deflate/inflate streams using
//!   the `flate2` crate (a thin wrapper over zlib)
//!
//! The per-message wrapper handles the RFC 7692 trailing bytes
//! (`0x00 0x00 0xFF 0xFF`) automatically.

pub mod deflate;
pub mod extensions;

pub use deflate::{
    CompressedMessage, DeflateContext, DecompressedContext, DecompressedMessage,
    NoContextTakeover,
};
pub use extensions::{
    parse_extensions_header, ExtensionParam, NegotiatedExtensions, WebSocketExtension,
    PERMESSAGE_DEFLATE,
};

/// Compression configuration for a WebSocket route. Mirrors the
/// relevant fields from RFC 7692 § 7.1.
///
/// The two `no_context_takeover` flags mean "reset the zlib state
/// after every message". Disabling context takeover uses slightly
/// more CPU but bounds the per-connection memory to one message
/// and slightly improves compression on bursty workloads.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// If `false`, the gateway never negotiates permessage-deflate
    /// even if both peers offer it. Useful for ops to disable
    /// compression without changing route definitions.
    pub enabled: bool,
    /// Server → Client direction: reset zlib state per message
    /// when the client requests it.
    pub server_no_context_takeover: bool,
    /// Client → Server direction: reset zlib state per message
    /// when the server (backend) requests it.
    pub client_no_context_takeover: bool,
    /// Window bits cap (8..=15). `None` means accept whatever
    /// the client offers (default 15).
    pub max_window_bits: Option<u8>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_no_context_takeover: false,
            client_no_context_takeover: false,
            max_window_bits: None,
        }
    }
}

impl CompressionConfig {
    /// Negotiate the actual parameters against a peer's offer.
    ///
    /// `peer_offer` is the `Sec-WebSocket-Extensions` header value
    /// from the side we're connecting TO (so for client→backend
    /// it's the backend's offer, and for backend→client it's the
    /// client's offer). Returns `Some(negotiated)` if both sides
    /// agree on permessage-deflate, `None` otherwise.
    pub fn negotiate(&self, peer_offer: &str) -> Option<NegotiatedExtensions> {
        if !self.enabled {
            return None;
        }
        let offered = extensions::parse_extensions_header(peer_offer);
        let pmce = offered.iter().find(|e| e.name == PERMESSAGE_DEFLATE)?;
        // Apply server-side no_context_takeover if config says so
        // or if the peer requested it. The gateway's view of
        // "server" depends on direction; for now we treat both
        // sides symmetrically.
        let server_no = self.server_no_context_takeover
            || pmce.has_param("server_no_context_takeover");
        let client_no = self.client_no_context_takeover
            || pmce.has_param("client_no_context_takeover");
        // Cap window bits at our max, but never raise it.
        let window_bits = match (self.max_window_bits, pmce.param("max_window_bits")) {
            (Some(cap), Some(offered)) => {
                let offered = offered.parse::<u8>().ok()?;
                Some(offered.min(cap).max(8))
            }
            (Some(cap), None) => Some(cap.max(8)),
            (None, Some(offered)) => {
                let v = offered.parse::<u8>().ok()?;
                Some(v.clamp(8, 15))
            }
            (None, None) => None,
        };
        Some(NegotiatedExtensions {
            server_no_context_takeover: server_no,
            client_no_context_takeover: client_no,
            window_bits,
        })
    }
}

/// Serializable WebSocket compression configuration attached to a
/// [`Router`]. Deserialized from JSON under the `ws_compression` key.
///
/// All fields are optional with sensible defaults so a minimal
/// config like `{"enabled": true}` is enough.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RouterCompressionConfig {
    /// Master switch.
    #[serde(default)]
    pub enabled: bool,
    /// Reset the zlib state after every server→client message
    /// (slightly worse compression, lower per-connection memory).
    #[serde(default)]
    pub server_no_context_takeover: bool,
    /// Reset the zlib state after every client→server message.
    #[serde(default)]
    pub client_no_context_takeover: bool,
    /// Window bits cap. Accepts 8..=15. `None` means 15 (zlib max).
    #[serde(default)]
    pub max_window_bits: Option<u8>,
}

impl RouterCompressionConfig {
    /// Convert to the runtime [`CompressionConfig`].
    pub fn to_compression_config(&self) -> CompressionConfig {
        CompressionConfig {
            enabled: self.enabled,
            server_no_context_takeover: self.server_no_context_takeover,
            client_no_context_takeover: self.client_no_context_takeover,
            max_window_bits: self.max_window_bits,
        }
    }

    /// Reject configs that don't make sense.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(w) = self.max_window_bits {
            if !(8..=15).contains(&w) {
                return Err(format!(
                    "ws_compression.max_window_bits must be in 8..=15, got {}",
                    w
                ));
            }
        }
        Ok(())
    }
}
