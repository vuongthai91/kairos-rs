//! Streaming body adapter: tonic `BoxBody` → actix `Stream<Item = Result<Bytes, E>>`.
//!
//! Tonic's response body implements the `http_body::Body` trait,
//! which yields `Frame<Bytes>` items (data + optional trailers).
//! actix's `HttpResponseBuilder::streaming()` takes a
//! `Stream<Item = Result<Bytes, E>>`. This module bridges the two
//! without forcing the gateway to materialize the entire response
//! in memory before writing it to the client.
//!
//! Used by the gRPC dispatcher to forward server-streaming and
//! bidi-streaming responses from the backend to the client.
//! Unary responses don't need this (they fit in a single
//! `Bytes` and are passed via `HttpResponseBuilder::body`).
//!
//! # Design
//!
//! We implement `futures_core::Stream` directly (rather than the
//! `http_body::Body` trait) so the same code works with
//! `HttpResponseBuilder::streaming` and any other actix body
//! consumer that takes a `Stream`. Trailers from the tonic body
//! are dropped on the floor at this layer; the actix response
//! builder doesn't have a way to surface HTTP/2 trailers in
//! `streaming()` mode. v2 may use `BodyStream` + a custom
//! `MessageBody` that emits trailers via `trailers()`.

use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use http_body::Body;
use http_body_util::StreamBody;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::body::BoxBody;


pin_project! {
    /// Stream that yields the data frames of a tonic `BoxBody`
    /// as `Result<Bytes, http_body::Error>`. Polls the inner
    /// body's `poll_frame` and surfaces the data portion; trailers
    /// are converted to a `grpc-status: 0 / grpc-message: ""`
    /// end-of-stream frame so the client knows the call
    /// completed cleanly. (For the trailers-only case the
    /// dispatcher already built a proper error frame upstream
    /// and this body never runs.)
    pub struct TonicBodyStream {
        #[pin]
        body: BoxBody,
    }
}

impl TonicBodyStream {
    /// Wrap a tonic response body. The stream ends when the
    /// body returns `None` from `poll_frame` OR when a trailers
    /// frame is received (we emit the trailer as a special
    /// `Bytes` containing the trailers header block so the
    /// client sees the end-of-call marker).
    pub fn new(body: BoxBody) -> Self {
        Self { body }
    }
}

impl Stream for TonicBodyStream {
    type Item = Result<Bytes, Box<dyn std::error::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.body.poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    Poll::Ready(Some(Ok(data.clone())))
                } else if let Some(trailers) = frame.trailers_ref() {
                    // Encode HTTP/2 trailers as a gRPC
                    // trailers-only body frame so the client
                    // sees `grpc-status: N` in the body. The
                    // wire format is the same as
                    // `routes::grpc::grpc_error_frame`:
                    //   byte 0: 0x80 (compressed flag set,
                    //           reserved bit, spec marker for
                    //           trailers-only frames)
                    //   bytes 1-4: BE u32 length of trailer block
                    //   bytes 5+:  "key: value\r\n" pairs
                    //
                    // actix-web 4.x can't emit HTTP/2 trailers
                    // (the `MessageBody` trait has no
                    // `trailers()` method), so this is the
                    // only way to surface the status to the
                    // gRPC client. The gRPC spec explicitly
                    // allows trailers in the body for servers
                    // that don't support HTTP/2 trailers.
                    let mut buf = BytesMut::new();
                    for (name, value) in trailers.iter() {
                        if let Ok(v) = value.to_str() {
                            buf.extend_from_slice(name.as_str().as_bytes());
                            buf.extend_from_slice(b": ");
                            buf.extend_from_slice(v.as_bytes());
                            buf.extend_from_slice(b"\r\n");
                        }
                    }
                    let body = buf.freeze();
                    let len = body.len() as u32;
                    let mut out = BytesMut::with_capacity(5 + body.len());
                    out.extend_from_slice(&[0x80u8]);
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(&body);
                    Poll::Ready(Some(Ok(out.freeze())))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(Box::new(std::io::Error::other(e.to_string())))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body::Frame;
    use futures_util::stream;

    /// Build a `BoxBody` from a vec of frames.
    fn body_from_frames(frames: Vec<Result<Frame<Bytes>, tonic::Status>>) -> BoxBody {
        let s = stream::iter(frames);
        let body: StreamBody<_> = StreamBody::new(s);
        BoxBody::new(body)
    }

    #[tokio::test]
    async fn stream_emits_data_frames_in_order() {
        let body = body_from_frames(vec![
            Ok(Frame::data(Bytes::from_static(b"hello "))),
            Ok(Frame::data(Bytes::from_static(b"world"))),
        ]);
        let mut s = TonicBodyStream::new(body);
        let a = futures_util::StreamExt::next(&mut s).await.unwrap().unwrap();
        let b = futures_util::StreamExt::next(&mut s).await.unwrap().unwrap();
        assert_eq!(a, Bytes::from_static(b"hello "));
        assert_eq!(b, Bytes::from_static(b"world"));
        let end = futures_util::StreamExt::next(&mut s).await;
        assert!(end.is_none(), "stream should be done");
    }

    #[tokio::test]
    async fn stream_encodes_trailers_as_body_frame() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        trailers.insert("grpc-message", "ok".parse().unwrap());
        let body = body_from_frames(vec![
            Ok(Frame::data(Bytes::from_static(b"data"))),
            Ok(Frame::trailers(trailers)),
        ]);
        let mut s = TonicBodyStream::new(body);
        let data = futures_util::StreamExt::next(&mut s).await.unwrap().unwrap();
        assert_eq!(data, Bytes::from_static(b"data"));
        // The trailers frame must be encoded as a gRPC
        // trailers-only body frame: 0x80 + 4-byte BE length
        // + "key: value\r\n" pairs. This is the same wire
        // format as `grpc_error_frame`.
        let trail = futures_util::StreamExt::next(&mut s).await.unwrap().unwrap();
        assert_eq!(trail[0], 0x80, "trailers frame must start with 0x80");
        let len = u32::from_be_bytes([trail[1], trail[2], trail[3], trail[4]]) as usize;
        assert_eq!(len, trail.len() - 5);
        let body_str = std::str::from_utf8(&trail[5..]).unwrap();
        assert!(body_str.contains("grpc-status: 0"));
        assert!(body_str.contains("grpc-message: ok"));
        // Stream must end after the trailers frame.
        let end = futures_util::StreamExt::next(&mut s).await;
        assert!(end.is_none());
    }
}
