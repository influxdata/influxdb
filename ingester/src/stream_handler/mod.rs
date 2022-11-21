//! An implementation of the "consumer-end" of the shard - pulling messages
//! from the shard, decoding the [`DmlOperation`] within and applying them
//! to in-memory state.
//!
//! A [`SequencedStreamHandler`] is responsible for consuming messages from the
//! shard (through the [`WriteBufferReading`] interface), decoding them to
//! [`DmlOperation`] instances and pushing them to the [`DmlSink`] for further
//! processing and buffering.
//!
//! The ingest/reads performed by the [`SequencedStreamHandler`] are rate
//! limited by the [`LifecycleManager`] it is initialised by, pausing until
//! [`LifecycleHandle::can_resume_ingest()`] returns true.
//!
//! [`SequencedStreamHandler`]: handler::SequencedStreamHandler
//! [`DmlOperation`]: dml::DmlOperation
//! [`WriteBufferReading`]: write_buffer::core::WriteBufferReading
//! [`LifecycleManager`]: crate::lifecycle::LifecycleManager
//! [`LifecycleHandle::can_resume_ingest()`]: crate::lifecycle::LifecycleHandle::can_resume_ingest()
//! [`DmlSink`]: crate::dml_sink::DmlSink

pub(crate) mod handler;
mod periodic_watermark_fetcher;

#[cfg(test)]
pub mod mock_watermark_fetcher;
pub(crate) mod sink_adaptor;
pub(crate) mod sink_instrumentation;
pub(crate) use periodic_watermark_fetcher::*;
