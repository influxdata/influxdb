use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use generated_types::{
    influxdata::iox::gossip::v1::{gossip_message::Msg, GossipMessage},
    prost::Message,
};
use parking_lot::Mutex;
use test_helpers::timeout::FutureTimeout;

use super::traits::SchemaBroadcast;

#[derive(Debug, Default)]
pub struct MockSchemaBroadcast {
    payloads: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl SchemaBroadcast for Arc<MockSchemaBroadcast> {
    async fn broadcast(&self, payload: Vec<u8>) {
        self.payloads.lock().push(payload);
    }
}

impl MockSchemaBroadcast {
    /// Return the raw, serialised payloads.
    pub fn raw_payloads(&self) -> Vec<Vec<u8>> {
        self.payloads.lock().clone()
    }

    /// Return the deserialised [`Msg`].
    pub fn messages(&self) -> Vec<Msg> {
        self.payloads
            .lock()
            .iter()
            .map(|v| {
                GossipMessage::decode(v.as_slice())
                    .map(|v| v.msg.expect("no message in payload"))
                    .expect("invalid gossip payload")
            })
            .collect()
    }

    pub async fn wait_for_messages(&self, count: usize) {
        let res = async {
            loop {
                if self.payloads.lock().len() >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .with_timeout(Duration::from_secs(5))
        .await;

        if res.is_err() {
            panic!(
                "never observed required number of messages: {:?}",
                &self.payloads
            );
        }
    }
}
