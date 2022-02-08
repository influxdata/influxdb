#![allow(missing_docs)]

use std::sync::{mpsc::SyncSender, Arc};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::kafka_topic_list::{
    api::KafkaTopicListApi,
    resources::{KafkaTopicList, KafkaTopicListStatus},
};

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MockKafkaTopicListApiCall {
    Get(String),
    PatchStatus {
        kafka_topic_list_name: String,
        status: KafkaTopicListStatus,
    },
}

#[derive(Debug, Default)]
pub struct ClientInner {
    /// A channel to push call notifications into as they occur.
    pub notify: Option<SyncSender<MockKafkaTopicListApiCall>>,

    /// A vector of calls in call order for assertions.
    pub calls: Vec<MockKafkaTopicListApiCall>,

    // Return values
    pub get_ret: Vec<Result<KafkaTopicList, kube::Error>>,
    pub patch_status_ret: Vec<Result<KafkaTopicList, kube::Error>>,
}

impl ClientInner {
    fn record_call(&mut self, c: MockKafkaTopicListApiCall) {
        self.calls.push(c.clone());
        if let Some(ref n) = self.notify {
            let _ = n.send(c);
        }
    }
}

impl From<ClientInner> for MockKafkaTopicListApi {
    fn from(state: ClientInner) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }
}

/// Mock helper to record a call and return the pre-configured value.
///
/// Pushes `$call` to call record, popping `self.$return` and returning it to
/// the caller. If no value exists, the pop attempt causes a panic.
macro_rules! record_and_return {
    ($self:ident, $call:expr, $return:ident) => {{
        let mut state = $self.state.lock();
        state.record_call($call);
        state.$return.pop().expect("no mock result to return")
    }};
}

#[derive(Debug, Default)]
pub struct MockKafkaTopicListApi {
    pub state: Arc<Mutex<ClientInner>>,
}

impl MockKafkaTopicListApi {
    pub fn with_notify(self, s: SyncSender<MockKafkaTopicListApiCall>) -> Self {
        self.state.lock().notify = Some(s);
        self
    }

    pub fn with_get_ret(self, ret: Vec<Result<KafkaTopicList, kube::Error>>) -> Self {
        self.state.lock().get_ret = ret;
        self
    }

    pub fn with_patch_status_ret(self, ret: Vec<Result<KafkaTopicList, kube::Error>>) -> Self {
        self.state.lock().patch_status_ret = ret;
        self
    }

    pub fn get_calls(&self) -> Vec<MockKafkaTopicListApiCall> {
        self.state.lock().calls.clone()
    }
}

#[async_trait]
impl KafkaTopicListApi for Arc<MockKafkaTopicListApi> {
    /// Gets a KafkaTopicList resource by name.
    async fn get_kafka_topic_list(
        &self,
        kafka_topic_list_name: String,
    ) -> Result<KafkaTopicList, kube::Error> {
        record_and_return!(
            self,
            MockKafkaTopicListApiCall::Get(kafka_topic_list_name,),
            get_ret
        )
    }

    /// Patch status block, if it exists, with the given status.
    async fn patch_resource_status(
        &self,
        kafka_topic_list_name: String,
        status: KafkaTopicListStatus,
    ) -> Result<KafkaTopicList, kube::Error> {
        record_and_return!(
            self,
            MockKafkaTopicListApiCall::PatchStatus {
                kafka_topic_list_name,
                status,
            },
            patch_status_ret
        )
    }
}

/// Cloning a client shares the same mock state across both client instances.
impl Clone for MockKafkaTopicListApi {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}
