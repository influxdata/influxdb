use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

use async_trait::async_trait;

use observability_deps::tracing::{error, info};
use trace::span::Span;

use crate::export::AsyncExport;
use crate::thrift::agent::{AgentSyncClient, TAgentSyncClient};
use crate::thrift::jaeger;
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};

mod span;

/// `JaegerAgentExporter` receives span data and writes it over UDP to a local jaeger agent
///
/// Note: will drop data if the UDP socket would block
pub struct JaegerAgentExporter {
    /// The name of the service
    service_name: String,

    /// The agent client that encodes messages
    client:
        AgentSyncClient<TCompactInputProtocol<NoopReader>, TCompactOutputProtocol<MessageWriter>>,

    /// Spans should be assigned a sequential sequence number
    /// to allow jaeger to better detect dropped spans
    next_sequence: i64,
}

impl JaegerAgentExporter {
    pub fn new<E: ToSocketAddrs + std::fmt::Display>(
        service_name: String,
        agent_endpoint: E,
    ) -> super::Result<Self> {
        info!(%agent_endpoint, %service_name, "Creating jaeger tracing exporter");
        let remote_addr = agent_endpoint.to_socket_addrs()?.next().ok_or_else(|| {
            super::Error::ResolutionError {
                address: agent_endpoint.to_string(),
            }
        })?;

        let local_addr: SocketAddr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()
        .unwrap();

        let socket = UdpSocket::bind(local_addr)?;
        socket.set_nonblocking(true)?;
        socket.connect(remote_addr)?;

        let client = AgentSyncClient::new(
            TCompactInputProtocol::new(NoopReader::default()),
            TCompactOutputProtocol::new(MessageWriter::new(socket)),
        );

        Ok(Self {
            service_name,
            client,
            next_sequence: 0,
        })
    }

    fn make_batch(&mut self, spans: Vec<Span>) -> jaeger::Batch {
        let seq_no = Some(self.next_sequence);
        self.next_sequence += 1;
        jaeger::Batch {
            process: jaeger::Process {
                service_name: self.service_name.clone(),
                tags: None,
            },
            spans: spans.into_iter().map(Into::into).collect(),
            seq_no,
            stats: None,
        }
    }
}

#[async_trait]
impl AsyncExport for JaegerAgentExporter {
    async fn export(&mut self, spans: Vec<Span>) {
        let batch = self.make_batch(spans);
        if let Err(e) = self.client.emit_batch(batch) {
            error!(%e, "error writing batch to jaeger agent")
        }
    }
}

/// `NoopReader` is a `std::io::Read` that never returns any data
#[derive(Debug, Default)]
struct NoopReader {}

impl std::io::Read for NoopReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

/// A `MessageWriter` only writes entire message payloads to the provided UDP socket
///
/// If the UDP socket would block, drops the packet
struct MessageWriter {
    buf: Vec<u8>,
    socket: UdpSocket,
}

impl MessageWriter {
    fn new(socket: UdpSocket) -> Self {
        Self {
            buf: vec![],
            socket,
        }
    }
}

impl std::io::Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let message_len = self.buf.len();
        let r = self.socket.send(&self.buf);
        self.buf.clear();
        match r {
            Ok(written) => {
                if written != message_len {
                    // In the event a message is truncated, there isn't an obvious way to recover
                    //
                    // The Thrift protocol is normally used on top of a reliable stream,
                    // e.g. TCP, and it is a bit of a hack to send it over UDP
                    //
                    // Jaeger requires that each thrift Message is encoded in exactly one UDP
                    // packet, as this ensures it either arrives in its entirety or not at all
                    //
                    // If for whatever reason the packet is truncated, the agent will fail to
                    // to decode it, likely due to a missing stop-field, and discard it
                    error!(%written, %message_len, "jaeger agent exporter failed to write message as single UDP packet");
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                error!("jaeger agent exporter would have blocked - dropping message");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thrift::agent::{AgentSyncHandler, AgentSyncProcessor};
    use chrono::{TimeZone, Utc};
    use std::sync::{Arc, Mutex};
    use thrift::server::TProcessor;
    use thrift::transport::TBufferChannel;
    use trace::ctx::{SpanContext, SpanId, TraceId};
    use trace::span::{SpanEvent, SpanStatus};

    struct TestHandler {
        batches: Arc<Mutex<Vec<jaeger::Batch>>>,
    }

    impl AgentSyncHandler for TestHandler {
        fn handle_emit_zipkin_batch(
            &self,
            _spans: Vec<crate::thrift::zipkincore::Span>,
        ) -> thrift::Result<()> {
            unimplemented!()
        }

        fn handle_emit_batch(&self, batch: jaeger::Batch) -> thrift::Result<()> {
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }
    }

    /// Wraps a UdpSocket and a buffer the size of the max UDP datagram and provides
    /// `std::io::Read` on this buffer's contents, ensuring that reads are not truncated
    struct Reader {
        socket: UdpSocket,
        buffer: Box<[u8; 65535]>,
        idx: usize,
        len: usize,
    }

    impl Reader {
        pub fn new(socket: UdpSocket) -> Self {
            Self {
                socket,
                buffer: Box::new([0; 65535]),
                idx: 0,
                len: 0,
            }
        }
    }

    impl std::io::Read for Reader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.idx == self.len {
                self.idx = 0;
                self.len = self.socket.recv(self.buffer.as_mut())?;
            }
            let to_read = buf.len().min(self.len - self.idx);
            buf.copy_from_slice(&self.buffer[self.idx..(self.idx + to_read)]);
            self.idx += to_read;
            Ok(to_read)
        }
    }

    #[tokio::test]
    async fn test_jaeger() {
        let server = UdpSocket::bind("0.0.0.0:0").unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();

        let address = server.local_addr().unwrap();
        let mut exporter = JaegerAgentExporter::new("service_name".to_string(), address).unwrap();

        let batches = Arc::new(Mutex::new(vec![]));

        let mut processor_input = TCompactInputProtocol::new(Reader::new(server));
        let mut processor_output = TCompactOutputProtocol::new(TBufferChannel::with_capacity(0, 0));
        let processor = AgentSyncProcessor::new(TestHandler {
            batches: Arc::clone(&batches),
        });

        let ctx = SpanContext {
            trace_id: TraceId::new(43434).unwrap(),
            parent_span_id: None,
            span_id: SpanId::new(3495993).unwrap(),
            collector: None,
        };
        let mut span = ctx.child("foo");
        span.status = SpanStatus::Ok;
        span.events = vec![SpanEvent {
            time: Utc.timestamp_nanos(200000),
            msg: "hello".into(),
        }];
        span.start = Some(Utc.timestamp_nanos(100000));
        span.end = Some(Utc.timestamp_nanos(300000));

        exporter.export(vec![span.clone(), span.clone()]).await;
        exporter.export(vec![span.clone()]).await;

        processor
            .process(&mut processor_input, &mut processor_output)
            .unwrap();

        processor
            .process(&mut processor_input, &mut processor_output)
            .unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 2);

        let b1 = &batches[0];

        assert_eq!(b1.spans.len(), 2);
        assert_eq!(b1.process.service_name.as_str(), "service_name");
        assert_eq!(b1.seq_no.unwrap(), 0);

        let b2 = &batches[1];
        assert_eq!(b2.spans.len(), 1);
        assert_eq!(b2.process.service_name.as_str(), "service_name");
        assert_eq!(b2.seq_no.unwrap(), 1);

        let b1_s0 = &b1.spans[0];

        assert_eq!(b1_s0, &b1.spans[1]);
        assert_eq!(b1_s0, &b2.spans[0]);

        assert_eq!(b1_s0.span_id, span.ctx.span_id.get() as i64);
        assert_eq!(
            b1_s0.parent_span_id,
            span.ctx.parent_span_id.unwrap().get() as i64
        );

        // microseconds not nanoseconds
        assert_eq!(b1_s0.start_time, 100);
        assert_eq!(b1_s0.duration, 200);

        let logs = b1_s0.logs.as_ref().unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].timestamp, 200);
        assert_eq!(logs[0].fields.len(), 1);
        assert_eq!(logs[0].fields[0].key.as_str(), "event");
        assert_eq!(logs[0].fields[0].v_str.as_ref().unwrap().as_str(), "hello");

        let tags = b1_s0.tags.as_ref().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].key.as_str(), "ok");
        assert!(tags[0].v_bool.unwrap());
    }

    #[test]
    fn test_resolve() {
        JaegerAgentExporter::new("service_name".to_string(), "localhost:8082").unwrap();
    }
}
