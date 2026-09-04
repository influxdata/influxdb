mod retryable_object_store;

pub use retryable_object_store::RetryParams;
pub use retryable_object_store::RetryableObjectStore;
pub use retryable_object_store::set_default_retry_params;

mod adaptive_put;
pub use adaptive_put::{AdaptivePutExt, DEFAULT_MULTIPART_CHUNK_SIZE};

mod adaptive_get;
pub use adaptive_get::{
    AdaptiveGetExt, DEFAULT_GET_CHUNK_SIZE, MAX_CHUNKS_PER_BATCH, SINGLE_GET_THRESHOLD,
};

mod self_verifying_create;
pub use self_verifying_create::{
    CONFLICT_METRIC_NAME, NonceCheck, PUT_NONCE_METADATA_KEY, PutNonce, SELF_WRITE_METRIC_NAME,
    SelfVerifyingCreate, SelfVerifyingCreateStore, UNTAGGED_CONFLICT_METRIC_NAME,
    UNVERIFIED_CONFLICT_METRIC_NAME, nonce_check,
};

#[cfg(any(feature = "test-helpers", test))]
mod test_object_store;
#[cfg(any(feature = "test-helpers", test))]
pub use test_object_store::{
    ErrorConfig, ErrorType, OperationContext, OperationKind, TestObjectStore,
};

#[cfg(any(feature = "test-helpers", test))]
mod cas_test_stores;
#[cfg(any(feature = "test-helpers", test))]
pub use cas_test_stores::{LostResponseError, LostResponseStore, VersionKeyedStore};
