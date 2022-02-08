use async_trait::async_trait;
use kube::{
    api::{Patch, PatchParams},
    Api,
};
use serde_json::json;

use crate::kafka_topic_list::resources::{KafkaTopicList, KafkaTopicListStatus};

#[async_trait]
pub trait KafkaTopicListApi: Send + Sync + Clone + 'static {
    /// Gets a KafkaTopicList resource by name.
    async fn get_kafka_topic_list(
        &self,
        kafka_topic_list_name: String,
    ) -> Result<KafkaTopicList, kube::Error>;

    /// Patch status block, if it exists, with the given status.
    async fn patch_resource_status(
        &self,
        kafka_topic_list_name: String,
        status: KafkaTopicListStatus,
    ) -> Result<KafkaTopicList, kube::Error>;
}

#[async_trait]
impl KafkaTopicListApi for Api<KafkaTopicList> {
    async fn get_kafka_topic_list(
        &self,
        kafka_topic_list_name: String,
    ) -> Result<KafkaTopicList, kube::Error> {
        self.get(kafka_topic_list_name.as_str()).await
    }

    async fn patch_resource_status(
        &self,
        kafka_topic_list_name: String,
        status: KafkaTopicListStatus,
    ) -> Result<KafkaTopicList, kube::Error> {
        let patch_params = PatchParams::default();
        let s = json!({ "status": status });
        self.patch_status(
            kafka_topic_list_name.as_str(),
            &patch_params,
            &Patch::Merge(&s),
        )
        .await
    }
}
