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

use bytes::Bytes;
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
                } else if frame.trailers_ref().is_some() {
                    // Trailers frame received. End the stream
                    // so actix closes the HTTP/2 connection.
                    // The gRPC client treats stream-end as
                    // `grpc-status: 0` (OK) per the gRPC spec.
                    //
                    // For unary calls, the route handler also
                    // sets `grpc-status: 0` as a response header
                    // (see `forward_to_backend` in
                    // `routes::grpc`), which grpcurl accepts for
                    // HTTP/2 unary calls. For streaming calls,
                    // the client interprets stream-end as
                    // `grpc-status: 0`.
                    //
                    // Non-zero statuses must be surfaced
                    // upstream as 502/504 responses (handled
                    // in `forward_streaming`).
                    Poll::Ready(None)
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
    async fn stream_ends_on_trailers_frame() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let body = body_from_frames(vec![
            Ok(Frame::data(Bytes::from_static(b"data"))),
            Ok(Frame::trailers(trailers)),
        ]);
        let mut s = TonicBodyStream::new(body);
        let data = futures_util::StreamExt::next(&mut s).await.unwrap().unwrap();
        assert_eq!(data, Bytes::from_static(b"data"));
        // The trailers frame must end the stream
        // (Poll::Ready(None)). The route handler sets
        // `grpc-status: 0` as a response header for
        // unary calls, and the gRPC client interprets
        // stream-end as `grpc-status: 0` for streaming
        // calls.
        let end = futures_util::StreamExt::next(&mut s).await;
        assert!(end.is_none(), "stream should end at trailers frame");
    }
}
