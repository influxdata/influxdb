/// Contains the conversion logic from a `trace::span::Span` to `thrift::jaeger::Span`
use crate::thrift::jaeger::{self, SpanRef};
use trace::{
    ctx::TraceId,
    span::{MetaValue, Span, SpanEvent, SpanStatus},
};

/// Split [`TraceId`] into high and low part.
fn split_trace_id(trace_id: TraceId) -> (i64, i64) {
    let trace_id = trace_id.get();
    let trace_id_high = (trace_id >> 64) as i64;
    let trace_id_low = trace_id as i64;
    (trace_id_high, trace_id_low)
}

impl TryFrom<Span> for jaeger::Span {
    type Error = String;

    fn try_from(mut s: Span) -> Result<Self, Self::Error> {
        let (trace_id_high, trace_id_low) = split_trace_id(s.ctx.trace_id);

        // A parent span id of 0 indicates no parent span ID (span IDs are non-zero)
        let parent_span_id = s.ctx.parent_span_id.map(|id| id.get()).unwrap_or_default() as i64;

        let (start_time, duration) = match (s.start, s.end) {
            (Some(start), Some(end)) => (
                start.timestamp_nanos_opt().ok_or_else(|| {
                    format!("start timestamp cannot be represented as nanos: {start}")
                })? / 1000,
                (end - start).num_microseconds().expect("no overflow"),
            ),
            (Some(start), _) => (
                start.timestamp_nanos_opt().ok_or_else(|| {
                    format!("start timestamp cannot be represented as nanos: {start}")
                })? / 1000,
                0,
            ),
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
            false => {
                let mut md = s.metadata.into_iter().collect::<Vec<_>>();
                md.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
                Some(
                    md.into_iter()
                        .map(|(name, value)| tag_from_meta(name.to_string(), value))
                        .collect(),
                )
            }
        };

        let logs = match s.events.is_empty() {
            true => None,
            false => Some(
                s.events
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
        };

        let references = if s.ctx.links.is_empty() {
            None
        } else {
            Some(
                s.ctx
                    .links
                    .into_iter()
                    .map(|(trace_id, span_id)| {
                        // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk_exporters/jaeger.md#links
                        let (trace_id_high, trace_id_low) = split_trace_id(trace_id);
                        SpanRef {
                            ref_type: jaeger::SpanRefType::FollowsFrom,
                            trace_id_high,
                            trace_id_low,
                            span_id: span_id.get() as i64,
                        }
                    })
                    .collect(),
            )
        };

        Ok(Self {
            trace_id_low,
            trace_id_high,
            span_id: s.ctx.span_id.get() as i64,
            parent_span_id,
            operation_name: s.name.to_string(),
            references,
            flags: 0,
            start_time,
            duration,
            tags,
            logs,
        })
    }
}

impl TryFrom<SpanEvent> for jaeger::Log {
    type Error = String;

    fn try_from(event: SpanEvent) -> Result<Self, Self::Error> {
        let mut md = event.metadata.into_iter().collect::<Vec<_>>();
        md.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));

        Ok(Self {
            timestamp: event.time.timestamp_nanos_opt().ok_or_else(|| {
                format!("timestamp cannot be represented as nanos: {}", event.time)
            })? / 1000,
            fields: std::iter::once(jaeger::Tag {
                key: "event".to_string(),
                v_type: jaeger::TagType::String,
                v_str: Some(event.msg.to_string()),
                v_double: None,
                v_bool: None,
                v_long: None,
                v_binary: None,
            })
            .chain(md.into_iter().map(|(k, v)| tag_from_meta(k.to_string(), v)))
            .collect(),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_trace_id_integer_conversion() {
        // test case from
        // https://github.com/open-telemetry/opentelemetry-specification/blob/639c7443e78800b085d2c9826d1b300f5e81fded/specification/trace/sdk_exporters/jaeger.md#ids
        let trace_id = TraceId::new(0xFF00000000000000).unwrap();
        let (high, low) = split_trace_id(trace_id);
        assert_eq!(high, 0);
        assert_eq!(low, -72057594037927936);
    }
}
