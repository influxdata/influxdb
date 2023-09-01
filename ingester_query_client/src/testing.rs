//! Testing tools.
use trace::{
    ctx::{SpanContext, SpanId, TraceId},
    span::Span,
};

/// Assert that the given type implements the given interface.
///
/// # Example
/// ```no_run
/// # mod x {
/// # use ingester_query_client::assert_impl;
/// struct MyStruct;
///
/// trait MyTrait {}
///
/// impl MyTrait for MyStruct {}
///
/// assert_impl!(my_struct_send, MyStruct, Send);
/// assert_impl!(my_struct_sync, MyStruct, Sync);
/// assert_impl!(my_struct_trait, MyStruct, MyTrait);
/// # }
/// ```
///
/// ```compile_fail
/// # mod x {
/// # use ingester_query_client::assert_impl;
/// struct MyStruct;
///
/// trait MyTrait {}
///
/// assert_impl!(my_struct_trait, MyStruct, MyTrait);
/// # }
/// ```
#[macro_export]
macro_rules! assert_impl {
    ($name:ident, $type:ty, $trait:path) => {
        mod $name {
            use super::*;

            const fn assert_f<T: $trait>() {}

            #[allow(dead_code)]
            const _: () = assert_f::<$type>();
        }
    };
}

/// Generate [`Span`] for testing.
pub fn span() -> Span {
    SpanContext {
        trace_id: TraceId::new(1).unwrap(),
        parent_span_id: None,
        span_id: SpanId::new(2).unwrap(),
        links: vec![],
        collector: None,
        sampled: true,
    }
    .child("span")
}
