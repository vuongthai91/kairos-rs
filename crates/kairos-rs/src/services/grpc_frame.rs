//! gRPC wire-format frame codec (PR9).
//!
//! gRPC over HTTP/2 carries each message in a "Length-Prefixed Message":
//!
//! ```text
//! +---------+---------+---------+---------+---------+---------+
//! |   0     |  1-4    |   5     |  6 ...  |         |         |
//! +---------+---------+---------+---------+---------+---------+
//! | Compressed | Length (4 bytes BE) |  Payload (Length bytes) |
//! +---------+---------+---------+---------+---------+---------+
//! ```
//!
//! - Bit 0 of byte 0 is the `Compressed-Flag` (1 = compressed, 0 = plain).
//!   We never set it; the gateway is proto-agnostic and forwards bytes
//!   unchanged, so compression is the client's responsibility.
//! - Bytes 1-4 are the payload length in big-endian.
//! - Bytes 5..5+length are the protobuf-encoded message.
//!
//! Trailers are sent as HTTP/2 trailers (the `trailers-only` response uses
//! HTTP headers instead — see [`is_grpc_status`]). We parse the
//! `grpc-status` header out of either, since both are valid.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Maximum payload size we accept from a single gRPC frame. 4 MiB matches
/// the gRPC default. Anything larger is almost certainly a bug or attack.
pub const MAX_GRPC_PAYLOAD: usize = 4 * 1024 * 1024;

/// A single length-prefixed gRPC frame. `compressed` is always false in
/// this gateway (we don't negotiate compression; clients may use it on
/// their own channel, and the gateway forwards the bytes unchanged).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcFrame {
    /// Wire-format byte 0. Always 0 in this implementation.
    pub compressed: bool,
    /// Decoded payload. Empty `Bytes` is legal (zero-length message).
    pub payload: Bytes,
}

/// Errors raised when decoding a gRPC wire-format stream.
#[derive(Debug, thiserror::Error)]
pub enum GrpcFrameError {
    /// Frame header announced a payload larger than [`MAX_GRPC_PAYLOAD`].
    #[error("gRPC frame payload too large: {0} bytes (max {MAX_GRPC_PAYLOAD})")]
    PayloadTooLarge(usize),
    /// Stream ended mid-frame.
    #[error("gRPC stream ended mid-frame: needed {needed} more bytes")]
    Incomplete { needed: usize },
}

impl GrpcFrame {
    /// Encode `payload` into a wire-format frame. `compressed` is always
    /// `false` in this implementation; the parameter is kept for API
    /// symmetry with the decoder.
    pub fn encode(payload: Bytes, compressed: bool) -> Bytes {
        let mut out = BytesMut::with_capacity(5 + payload.len());
        // Byte 0: compressed flag (bit 0). Length upper bit must be 0
        // because gRPC spec limits payload to 2^32 - 1.
        out.put_u8(if compressed { 1 } else { 0 });
        // Bytes 1-4: payload length, big-endian.
        out.put_u32(payload.len() as u32);
        out.extend_from_slice(&payload);
        out.freeze()
    }

    /// Decode one frame from `buf`. Returns `Ok(Some(frame))` if a complete
    /// frame is present, `Ok(None)` if more bytes are needed, or
    /// `Err(_)` if the frame is malformed (e.g. oversized).
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Option<Self>, GrpcFrameError> {
        if buf.remaining() < 5 {
            return Ok(None);
        }
        // Peek the 5-byte header without consuming (so a partial frame
        // can be retried when more bytes arrive).
        let header = buf.chunk();
        let compressed = header[0] & 0x01 != 0;
        let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]])
            as usize;
        if length > MAX_GRPC_PAYLOAD {
            return Err(GrpcFrameError::PayloadTooLarge(length));
        }
        if buf.remaining() < 5 + length {
            return Ok(None);
        }
        // Consume the header.
        buf.advance(5);
        // Copy `length` bytes out of the buffer.
        let payload = buf.copy_to_bytes(length);
        Ok(Some(GrpcFrame { compressed, payload }))
    }
}

/// Standard gRPC status codes. Mirrors `tonic::Code` but kept local so the
/// codec stays independent of `tonic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum GrpcStatus {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcStatus {
    /// Parse the gRPC `grpc-status` trailer / response header.
    /// `Unknown` is returned for unrecognised codes (per gRPC spec).
    pub fn from_header(value: &str) -> Self {
        match value.trim() {
            "0" => GrpcStatus::Ok,
            "1" => GrpcStatus::Cancelled,
            "2" => GrpcStatus::Unknown,
            "3" => GrpcStatus::InvalidArgument,
            "4" => GrpcStatus::DeadlineExceeded,
            "5" => GrpcStatus::NotFound,
            "6" => GrpcStatus::AlreadyExists,
            "7" => GrpcStatus::PermissionDenied,
            "8" => GrpcStatus::ResourceExhausted,
            "9" => GrpcStatus::FailedPrecondition,
            "10" => GrpcStatus::Aborted,
            "11" => GrpcStatus::OutOfRange,
            "12" => GrpcStatus::Unimplemented,
            "13" => GrpcStatus::Internal,
            "14" => GrpcStatus::Unavailable,
            "15" => GrpcStatus::DataLoss,
            "16" => GrpcStatus::Unauthenticated,
            _ => GrpcStatus::Unknown,
        }
    }

    /// Numeric code as it appears on the wire.
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Return `true` if the given HTTP/2 `content-type` header value indicates
/// a gRPC request or response. The spec is `application/grpc` plus an
/// optional `+proto` / `+json` / etc. serialization suffix.
pub fn is_grpc_content_type(content_type: &str) -> bool {
    // Lowercase + trim so we don't allocate per-request.
    let lower = content_type.trim().to_ascii_lowercase();
    lower == "application/grpc" || lower.starts_with("application/grpc+")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn encode_decode_round_trip() {
        let payload = Bytes::from_static(b"hello");
        let wire = GrpcFrame::encode(payload.clone(), false);
        assert_eq!(wire.len(), 10);
        let mut buf = wire;
        let decoded = GrpcFrame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
        assert!(!decoded.compressed);
    }

    #[test]
    fn decode_returns_none_for_short_buffer() {
        let mut buf = Bytes::from_static(&[0u8, 0, 0, 0]);
        assert!(GrpcFrame::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_returns_none_for_partial_payload() {
        // Header says 5 bytes, only 2 follow.
        let mut buf = Bytes::from_static(&[0u8, 0, 0, 0, 5, b'a', b'b']);
        assert!(GrpcFrame::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_rejects_oversized_payload() {
        // Length = MAX_GRPC_PAYLOAD + 1
        let too_big = (MAX_GRPC_PAYLOAD as u32 + 1).to_be_bytes();
        let wire = vec![0u8, too_big[0], too_big[1], too_big[2], too_big[3]];
        let mut buf = Bytes::from(wire);
        match GrpcFrame::decode(&mut buf) {
            Err(GrpcFrameError::PayloadTooLarge(n)) => assert_eq!(n, MAX_GRPC_PAYLOAD + 1),
            other => panic!("expected PayloadTooLarge, got {:?}", other),
        }
    }

    #[test]
    fn decode_handles_compressed_flag() {
        // compressed = true
        let mut buf = Bytes::from_static(&[1u8, 0, 0, 0, 0]);
        let frame = GrpcFrame::decode(&mut buf).unwrap().unwrap();
        assert!(frame.compressed);
        assert_eq!(frame.payload.len(), 0);
    }

    #[test]
    fn decode_handles_zero_length_payload() {
        let mut buf = Bytes::from_static(&[0u8, 0, 0, 0, 0]);
        let frame = GrpcFrame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.payload.len(), 0);
        assert!(!frame.compressed);
    }

    #[test]
    fn grpc_status_from_header_known_codes() {
        assert_eq!(GrpcStatus::from_header("0"), GrpcStatus::Ok);
        assert_eq!(GrpcStatus::from_header("14"), GrpcStatus::Unavailable);
        assert_eq!(GrpcStatus::from_header(" 5 "), GrpcStatus::NotFound);
    }

    #[test]
    fn grpc_status_from_header_unknown() {
        assert_eq!(GrpcStatus::from_header("99"), GrpcStatus::Unknown);
        assert_eq!(GrpcStatus::from_header("not-a-number"), GrpcStatus::Unknown);
    }

    #[test]
    fn is_grpc_content_type_matches() {
        assert!(is_grpc_content_type("application/grpc"));
        assert!(is_grpc_content_type("application/grpc+proto"));
        assert!(is_grpc_content_type("application/grpc+json"));
        assert!(is_grpc_content_type("  Application/GRPC  "));
        assert!(!is_grpc_content_type("application/json"));
        assert!(!is_grpc_content_type("text/plain"));
        assert!(!is_grpc_content_type("application/grpcweb"));
        assert!(!is_grpc_content_type("application/grpc-web"));
    }
}
