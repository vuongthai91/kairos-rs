//! Integration test for the v2 response body pipeline.
//!
//! Verifies that `TonicBodyStream` correctly yields data
//! frames from a tonic `BoxBody` and ends the stream at the
//! trailers frame (the actix-web 4.x workaround for the
//! missing `MessageBody::trailers()` method).
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use tonic::body::BoxBody;

#[tokio::test]
async fn tonic_body_stream_yields_data_then_ends_at_trailers() {
    use kairos_rs::services::grpc_stream::TonicBodyStream;

    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().unwrap());
    let frames: Vec<Result<Frame<Bytes>, tonic::Status>> = vec![
        Ok(Frame::data(Bytes::from_static(b"hello"))),
        Ok(Frame::data(Bytes::from_static(b"world"))),
        Ok(Frame::trailers(trailers)),
    ];
    let body: BoxBody = BoxBody::new(StreamBody::new(futures_util::stream::iter(frames)));

    let mut stream = TonicBodyStream::new(body);
    let a = stream.next().await.unwrap().unwrap();
    let b = stream.next().await.unwrap().unwrap();
    let end = stream.next().await;

    assert_eq!(a, Bytes::from_static(b"hello"));
    assert_eq!(b, Bytes::from_static(b"world"));
    // Stream must end at the trailers frame (Poll::Ready(None)).
    // The route handler sets grpc-status: 0 as a response header
    // for unary calls; streaming clients interpret stream-end
    // as grpc-status: 0.
    assert!(end.is_none(), "stream should end at trailers frame");
}

#[tokio::test]
async fn tonic_body_stream_handles_single_data_frame() {
    use kairos_rs::services::grpc_stream::TonicBodyStream;

    let frames: Vec<Result<Frame<Bytes>, tonic::Status>> = vec![
        Ok(Frame::data(Bytes::from_static(b"unary-response"))),
    ];
    let body: BoxBody = BoxBody::new(StreamBody::new(futures_util::stream::iter(frames)));

    let mut stream = TonicBodyStream::new(body);
    let a = stream.next().await.unwrap().unwrap();
    let end = stream.next().await;

    assert_eq!(a, Bytes::from_static(b"unary-response"));
    assert!(end.is_none());
}
