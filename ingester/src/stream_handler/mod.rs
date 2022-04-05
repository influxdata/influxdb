//! An implementation of the "consumer-end" of the sequencer - pulling messages
//! from the sequencer, decoding the [`DmlOperation`] within and applying them
//! to in-memory state.
//!
//! A [`SequencedStreamHandler`] is responsible for consuming messages from the
//! sequencer (through the [`WriteBufferReading`] interface), decoding them to
//! [`DmlOperation`] instances and pushing them to the [`DmlSink`] for further
//! processing and buffering.
//!
//! The ingest/reads performed by the [`SequencedStreamHandler`] are rate
//! limited by the [`LifecycleManager`] it is initialised by, pausing until
//! [`LifecycleHandle::can_resume_ingest()`] returns true.
//!
//! [`DmlOperation`]: dml::DmlOperation
//! [`WriteBufferReading`]: write_buffer::core::WriteBufferReading
//! [`LifecycleManager`]: crate::lifecycle::LifecycleManager
//! [`LifecycleHandle::can_resume_ingest()`]: crate::lifecycle::LifecycleHandle::can_resume_ingest()

mod handler;
mod periodic_watermark_fetcher;
mod sink;

#[cfg(test)]
pub mod mock_sink;

pub use handler::*;
pub use periodic_watermark_fetcher::*;
pub use sink::*;
