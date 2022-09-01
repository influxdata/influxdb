use std::io::Cursor;

use crate::end_to_end_cases::{
    server::TestService,
    test_utils::{Fixture, RecordingSink},
};
use assert_matches::assert_matches;
use grpc_binary_logger_proto::{
    grpc_log_entry::{EventType, Payload},
    ClientHeader, Message, Metadata, MetadataEntry, ServerHeader, Trailer,
};
use grpc_binary_logger_test_proto::{TestRequest, TestResponse};
use prost::Message as _;
use tonic::{metadata::MetadataValue, Code};

#[tokio::test]
async fn test_unary() {
    let sink = RecordingSink::new();
    let fixture = Fixture::new(TestService, sink.clone())
        .await
        .expect("fixture");

    const BASE: u64 = 1;
    {
        let mut client = fixture.client.clone();
        let mut req = tonic::Request::new(TestRequest { question: BASE });
        req.metadata_mut().insert(
            "my-client-header",
            MetadataValue::from_static("my-client-header-value"),
        );
        let res = client.test_unary(req).await.expect("no errors");
        assert_eq!(res.into_inner().answer, BASE + 1);
    }

    let entries = sink.entries();
    assert_eq!(entries.len(), 5);

    let entry = &entries[0];
    assert_eq!(entry.r#type(), EventType::ClientHeader);
    assert_matches!(
        entry.payload,
        Some(Payload::ClientHeader(ClientHeader {
            ref method_name,
            metadata: Some(Metadata{ref entry}), ..
        })) => {
            assert_eq!(method_name, "/test.Test/TestUnary");
            assert_matches!(entry[..], [
                MetadataEntry{ref key, ref value, ..},
            ] if key == "my-client-header" && value == b"my-client-header-value");
        }
    );

    let entry = &entries[1];
    assert_eq!(entry.r#type(), EventType::ClientMessage);
    assert_matches!(
        entry.payload,
        Some(Payload::Message(Message{length, ref data})) => {
            assert_eq!(data.len(), length as usize);
            let message = TestRequest::decode(Cursor::new(data)).expect("valid proto");
            assert_eq!(message.question, BASE);
        }
    );

    let entry = &entries[2];
    assert_eq!(entry.r#type(), EventType::ServerHeader);
    assert_matches!(
        entry.payload,
        Some(Payload::ServerHeader(ServerHeader {
            metadata: Some(Metadata{ref entry}), ..
        })) => {
            assert_matches!(entry[..], [
                MetadataEntry{ref key, ref value, ..},
            ] if key == "my-server-header" && value == b"my-server-header-value");
        }
    );

    let entry = &entries[3];
    assert_eq!(entry.r#type(), EventType::ServerMessage);
    assert_matches!(
        entry.payload,
        Some(Payload::Message(Message{length, ref data})) => {
            assert_eq!(data.len(), length as usize);
            let message = TestResponse::decode(Cursor::new(data)).unwrap();
            assert_eq!(message.answer, BASE+1);
        }
    );

    let entry = &entries[4];
    assert_eq!(entry.r#type(), EventType::ServerTrailer);
    assert_matches!(entry.payload, Some(Payload::Trailer(Trailer { .. })));
}

#[tokio::test]
async fn test_unary_error() {
    let sink = RecordingSink::new();
    let fixture = Fixture::new(TestService, sink.clone())
        .await
        .expect("fixture");

    {
        let mut client = fixture.client.clone();
        let err = client
            .test_unary(TestRequest { question: 42 })
            .await
            .expect_err("should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "The Answer is not a question");
    }

    let entries = sink.entries();
    assert_eq!(entries.len(), 4);

    let entry = &entries[3];
    assert_eq!(entry.r#type(), EventType::ServerTrailer);
    assert_matches!(
        entry.payload,
        Some(Payload::Trailer(Trailer { status_code, .. })) => {
            assert_eq!(status_code, Code::InvalidArgument as u32);
        }
    );
}
