use std::{sync::Arc, time::Duration};

use generated_types::influxdata::iox::gossip::v1::schema_message::Event;
use parking_lot::Mutex;
use test_helpers::timeout::FutureTimeout;

use super::traits::SchemaBroadcast;

#[derive(Debug, Default)]
pub struct MockSchemaBroadcast {
    payloads: Mutex<Vec<Event>>,
}

impl SchemaBroadcast for Arc<MockSchemaBroadcast> {
    fn broadcast(&self, payload: Event) {
        self.payloads.lock().push(payload);
    }
}

impl MockSchemaBroadcast {
    /// Return the broadcast [`Event`].
    pub fn messages(&self) -> Vec<Event> {
        self.payloads.lock().clone()
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
