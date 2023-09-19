//! Easy-to-use compositions of different [layers](crate::layer::Layer).

use std::sync::Arc;

use http::{HeaderName, Uri};

use crate::{
    interface::IngesterClient,
    layers::{
        deserialize::DeserializeLayer, logging::LoggingLayer, network::NetworkLayer,
        serialize::SerializeLayer,
    },
};

/// Simple network client w/o any retries.
pub fn simple_network_client(uri: Uri, trace_context_header_name: HeaderName) -> IngesterClient {
    let addr = Arc::from(uri.to_string());
    let l = NetworkLayer::new(uri);
    let l = SerializeLayer::new(l, trace_context_header_name);
    let l = DeserializeLayer::new(l);
    let l = LoggingLayer::new(l, addr);
    Arc::new(l)
}
