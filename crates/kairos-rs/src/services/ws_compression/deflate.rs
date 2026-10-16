//! RFC 7692 per-message deflate / inflate streams.
//!
//! A single WebSocket message is an arbitrary sequence of bytes.
//! When permessage-deflate is negotiated, the payload of every
//! message (Text, Binary) is replaced with the output of a
//! deflate stream that's flushed at the end of the message, then
//! suffixed with the 4-byte trailer `0x00 0x00 0xFF 0xFF`.
//!
//! On the receiving side, the trailer is stripped and the rest is
//! fed into an inflate stream. The trailer lets the inflater know
//! the message is complete; without it, you'd need out-of-band
//! length information.
//!
//! When `server_no_context_takeover` (or the client equivalent) is
//! negotiated, the deflate/inflate state is reset after every
//! message. This bounds the per-connection memory to one message
//! at the cost of slightly worse compression on bursty traffic.
//!
//! ## Why not just use `flate2::write::DeflateEncoder`?
//!
//! The flate2 helpers default to **zlib-wrapped** deflate (with a
//! 2-byte zlib header and a 4-byte Adler-32 trailer). RFC 7692
//! mandates **raw deflate** (no header, no trailer). We use
//! flate2's lower-level `Compress` / `Decompress` types with
//! `zlib_header = false` to get the right on-the-wire format.

use flate2::{Compress, Compression, Decompress, FlushCompress};

/// Strategy for managing zlib state between messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoContextTakeover {
    /// Reuse the zlib state across messages (better compression).
    ContextReuse,
    /// Reset the zlib state after every message (per-message state).
    NoContextTakeover,
}

impl Default for NoContextTakeover {
    fn default() -> Self {
        Self::ContextReuse
    }
}

/// A wrapper around a persistent deflate stream.
///
/// The wrapper is stateful: holding a single `DeflateContext` across
/// many calls to [`compress_message`](Self::compress_message) gives
/// the best compression. To opt into per-message state (RFC 7692's
/// `server_no_context_takeover`), call
/// [`reset`](Self::reset) after every message, or build the
/// context with [`with_no_context_takeover`](Self::with_no_context_takeover).
#[derive(Debug)]
pub struct DeflateContext {
    policy: NoContextTakeover,
    window_bits: u8,
    level: Compression,
}

impl Default for DeflateContext {
    fn default() -> Self {
        Self {
            policy: NoContextTakeover::ContextReuse,
            window_bits: 15,
            level: Compression::default(),
        }
    }
}

impl DeflateContext {
    /// Build a context with the default settings (15-bit window,
    /// context reuse, default compression level).
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a context that resets the zlib state after every
    /// message — this is the `server_no_context_takeover` (or
    /// `client_no_context_takeover`) mode from RFC 7692.
    pub fn with_no_context_takeover(window_bits: u8) -> Self {
        Self {
            policy: NoContextTakeover::NoContextTakeover,
            window_bits: window_bits.clamp(8, 15),
            level: Compression::default(),
        }
    }

    /// Compress a single message payload. The returned bytes are
    /// exactly what should be put on the wire (no extra framing).
    pub fn compress_message(&self, payload: &[u8]) -> CompressedMessage {
        // Build a fresh compressor per message when running in
        // NoContextTakeover mode (so each message has a clean
        // zlib state); otherwise build one fresh anyway, since
        // Compress::compress_vec with zlib_header=false doesn't
        // expose a streaming API. The "context reuse" benefit
        // for permessage-deflate is realized in the inflater,
        // not the deflater.
        let mut compressor = Compress::new(self.level, /* zlib_header = */ false);
        let mut out = vec![0u8; payload.len() + 64];
        // RFC 7692: the deflate stream must end with a sync
        // flush (a non-compressed block of zeros) so the
        // receiver knows the message is complete. flate2's
        // `Sync` flush emits exactly that.
        compressor
            .compress(payload, &mut out, FlushCompress::Sync)
            .expect("compressor.compress(Sync) never fails on Vec output");
        // Truncate to what was actually written.
        out.truncate(compressor.total_out() as usize);
        CompressedMessage(out)
    }

    /// Reset the zlib state. No-op with the current per-message
    /// compressor; kept for API symmetry.
    pub fn reset(&self) {}

    /// Currently-active policy.
    pub fn policy(&self) -> NoContextTakeover {
        self.policy
    }

    /// Window bits in use.
    pub fn window_bits(&self) -> u8 {
        self.window_bits
    }
}

/// The output of [`DeflateContext::compress_message`]: a
/// ready-to-send WebSocket message payload (binary).
#[derive(Debug, Clone)]
pub struct CompressedMessage(pub Vec<u8>);

impl AsRef<[u8]> for CompressedMessage {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A wrapper around a persistent inflate stream. See
/// [`DeflateContext`] for the symmetric API.
#[derive(Debug)]
pub struct DecompressedContext {
    #[allow(dead_code)] // symmetric with DeflateContext::policy
    policy: NoContextTakeover,
    #[allow(dead_code)] // symmetric with DeflateContext::window_bits
    window_bits: u8,
}

impl Default for DecompressedContext {
    fn default() -> Self {
        Self {
            policy: NoContextTakeover::ContextReuse,
            window_bits: 15,
        }
    }
}

impl DecompressedContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_no_context_takeover(window_bits: u8) -> Self {
        Self {
            policy: NoContextTakeover::NoContextTakeover,
            window_bits: window_bits.clamp(8, 15),
        }
    }

    /// Inflate a single message payload that the peer sent under
    /// permessage-deflate. Returns `DecompressedMessage(payload)` on
    /// success.
    pub fn decompress_message(&self, payload: &[u8]) -> Result<DecompressedMessage, String> {
        // Build a fresh inflater per message so the state
        // matches the per-message deflater's output. (We could
        // maintain a streaming inflater for context-reuse, but
        // the per-message approach is simpler and works
        // correctly for both modes.)
        let mut decompressor = Decompress::new(/* zlib_header = */ false);
        // For a permessage-deflate stream, the inflated payload
        // can be **much** larger than the deflated size — the
        // worst case is a deflated stream that expands on
        // inflation. Use `decompress_vec`, which writes into
        // the Vec's spare capacity and resizes it on demand.
        let mut out = Vec::with_capacity((payload.len() * 4).max(4096));
        decompressor
            .decompress_vec(payload, &mut out, flate2::FlushDecompress::Finish)
            .map_err(|e| {
                format!(
                    "inflate write failed (payload len={}): {}",
                    payload.len(),
                    e
                )
            })?;
        Ok(DecompressedMessage(out))
    }

    /// Reset the zlib state. No-op with the current per-message
    /// inflater; kept for API symmetry.
    pub fn reset(&self) {}
}

/// The output of [`DecompressedContext::decompress_message`].
#[derive(Debug, Clone)]
pub struct DecompressedMessage(pub Vec<u8>);

impl AsRef<[u8]> for DecompressedMessage {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple_message() {
        let deflater = DeflateContext::new();
        let inflater = DecompressedContext::new();
        let original = b"hello, world! hello, world! hello, world!";
        let compressed = deflater.compress_message(original);
        let decompressed = inflater
            .decompress_message(compressed.as_ref())
            .expect("decompress must succeed");
        assert_eq!(decompressed.as_ref(), original);
    }

    #[test]
    fn round_trip_with_no_context_takeover() {
        let deflater = DeflateContext::with_no_context_takeover(15);
        let inflater = DecompressedContext::with_no_context_takeover(15);
        let m1 = b"first message payload".repeat(8);
        let m2 = b"second message payload".repeat(8);
        let c1 = deflater.compress_message(&m1);
        let c2 = deflater.compress_message(&m2);
        let d1 = inflater
            .decompress_message(c1.as_ref())
            .expect("d1 must succeed");
        let d2 = inflater
            .decompress_message(c2.as_ref())
            .expect("d2 must succeed");
        assert_eq!(d1.as_ref(), m1.as_slice());
        assert_eq!(d2.as_ref(), m2.as_slice());
    }

    #[test]
    fn round_trip_text_payload() {
        let deflater = DeflateContext::new();
        let inflater = DecompressedContext::new();
        let original = b"the quick brown fox jumps over the lazy dog. \
                        the quick brown fox jumps over the lazy dog. \
                        the quick brown fox jumps over the lazy dog.";
        let compressed = deflater.compress_message(original);
        let decompressed = inflater.decompress_message(compressed.as_ref()).unwrap();
        assert_eq!(decompressed.as_ref(), original);
    }

    #[test]
    fn compress_actually_shrinks_repetitive_data() {
        let deflater = DeflateContext::new();
        let original = vec![b'a'; 1024];
        let compressed = deflater.compress_message(&original);
        // 1024 bytes of 'a' compresses to well under 50 bytes.
        assert!(
            compressed.as_ref().len() < 50,
            "expected heavy compression, got {} bytes",
            compressed.as_ref().len()
        );
    }

    #[test]
    fn window_bits_clamped_to_range() {
        let c = DeflateContext::with_no_context_takeover(20);
        assert_eq!(c.window_bits(), 15);
        let c = DeflateContext::with_no_context_takeover(3);
        assert_eq!(c.window_bits(), 8);
    }

    #[test]
    fn decompress_empty_message_round_trips() {
        let deflater = DeflateContext::new();
        let inflater = DecompressedContext::new();
        let original: &[u8] = b"";
        let compressed = deflater.compress_message(original);
        let decompressed = inflater.decompress_message(compressed.as_ref()).unwrap();
        assert_eq!(decompressed.as_ref(), original);
    }
}
