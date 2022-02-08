use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[kube(
    group = "iox.influxdata.com",
    version = "v1alpha1",
    kind = "KafkaTopicList",
    namespaced,
    shortname = "topics"
)]
#[kube(status = "KafkaTopicListStatus")]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicListSpec {
    topics: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicListStatus {
    conditions: Vec<KafkaTopicListStatusCondition>,
    observed_generation: i64, // type matches that of metadata.generation
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicListStatusCondition {
    type_: String,
    status: String,
    message: String,
    last_transition_time: String,
    last_update_time: String,
}

impl KafkaTopicListSpec {
    pub fn new(topics: Vec<String>) -> Self {
        Self { topics }
    }

    pub fn topics(&self) -> &Vec<String> {
        &self.topics
    }
}

impl KafkaTopicListStatus {
    pub fn conditions(&self) -> &Vec<KafkaTopicListStatusCondition> {
        &self.conditions
    }

    pub fn conditions_mut(&mut self) -> &mut Vec<KafkaTopicListStatusCondition> {
        &mut self.conditions
    }

    pub fn observed_generation(&self) -> i64 {
        self.observed_generation
    }

    pub fn set_observed_generation(&mut self, observed_generation: i64) {
        self.observed_generation = observed_generation;
    }
}

impl KafkaTopicListStatusCondition {
    pub fn new(
        type_: String,
        status: String,
        message: String,
        last_transition_time: String,
        last_update_time: String,
    ) -> Self {
        Self {
            type_,
            status,
            message,
            last_transition_time,
            last_update_time,
        }
    }

    pub fn type_(&self) -> &String {
        &self.type_
    }

    pub fn status(&self) -> &String {
        &self.status
    }

    pub fn message(&self) -> &String {
        &self.message
    }

    pub fn last_transition_time(&self) -> &String {
        &self.last_transition_time
    }

    pub fn last_update_time(&self) -> &String {
        &self.last_update_time
    }
}

impl PartialEq for KafkaTopicListStatusCondition {
    // just for assertions in tests; too tedious to have to have the items the same
    // too
    fn eq(&self, other: &Self) -> bool {
        self.type_ == other.type_ && self.status == other.status && self.message == other.message
    }
}
