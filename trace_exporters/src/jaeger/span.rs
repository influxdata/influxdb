/// Contains the conversion logic from a `trace::span::Span` to `thrift::jaeger::Span`
use crate::thrift::jaeger;
use trace::span::{MetaValue, Span, SpanEvent, SpanStatus};

impl From<Span> for jaeger::Span {
    fn from(mut s: Span) -> Self {
        let trace_id = s.ctx.trace_id.get();
        let trace_id_high = (trace_id >> 64) as i64;
        let trace_id_low = trace_id as i64;

        // A parent span id of 0 indicates no parent span ID (span IDs are non-zero)
        let parent_span_id = s.ctx.parent_span_id.map(|id| id.get()).unwrap_or_default() as i64;

        let (start_time, duration) = match (s.start, s.end) {
            (Some(start), Some(end)) => (
                start.timestamp_nanos() / 1000,
                (end - start).num_microseconds().expect("no overflow"),
            ),
            (Some(start), _) => (start.timestamp_nanos() / 1000, 0),
            _ => (0, 0),
        };

        // These don't appear to be standardised, however, the jaeger UI treats
        // the presence of an "error" tag as indicating an error
        match s.status {
            SpanStatus::Ok => {
                s.metadata
                    .entry("ok".into())
                    .or_insert(MetaValue::Bool(true));
            }
            SpanStatus::Err => {
                s.metadata
                    .entry("error".into())
                    .or_insert(MetaValue::Bool(true));
            }
            SpanStatus::Unknown => {}
        }

        let tags = match s.metadata.is_empty() {
            true => None,
            false => Some(
                s.metadata
                    .into_iter()
                    .map(|(name, value)| tag_from_meta(name.to_string(), value))
                    .collect(),
            ),
        };

        let logs = match s.events.is_empty() {
            true => None,
            false => Some(s.events.into_iter().map(Into::into).collect()),
        };

        Self {
            trace_id_low,
            trace_id_high,
            span_id: s.ctx.span_id.get() as i64,
            parent_span_id,
            operation_name: s.name.to_string(),
            references: None,
            flags: 0,
            start_time,
            duration,
            tags,
            logs,
        }
    }
}

impl From<SpanEvent> for jaeger::Log {
    fn from(event: SpanEvent) -> Self {
        Self {
            timestamp: event.time.timestamp_nanos() / 1000,
            fields: vec![jaeger::Tag {
                key: "event".to_string(),
                v_type: jaeger::TagType::String,
                v_str: Some(event.msg.to_string()),
                v_double: None,
                v_bool: None,
                v_long: None,
                v_binary: None,
            }],
        }
    }
}

fn tag_from_meta(key: String, value: MetaValue) -> jaeger::Tag {
    let mut tag = jaeger::Tag {
        key,
        v_type: jaeger::TagType::String,
        v_str: None,
        v_double: None,
        v_bool: None,
        v_long: None,
        v_binary: None,
    };

    match value {
        MetaValue::String(v) => {
            tag.v_type = jaeger::TagType::String;
            tag.v_str = Some(v.to_string())
        }
        MetaValue::Float(v) => {
            tag.v_type = jaeger::TagType::Double;
            tag.v_double = Some(v.into())
        }
        MetaValue::Int(v) => {
            tag.v_type = jaeger::TagType::Long;
            tag.v_long = Some(v)
        }
        MetaValue::Bool(v) => {
            tag.v_type = jaeger::TagType::Bool;
            tag.v_bool = Some(v)
        }
    };
    tag
}
