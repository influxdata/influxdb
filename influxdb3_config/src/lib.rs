//! Configuration Code for InfluxDB Monolith OSS/Pro
//! This crate handles our configuration that is not often changed, but
//! still needs to exist for the Monolith code to function.

mod pro;
pub use pro::*;

use object_store::{path::Path, ObjectStore, PutPayload};
use serde::Deserialize;
use serde::Serialize;
use std::future::Future;

mod private {
    pub trait Sealed {}
    impl Sealed for super::ProConfig {}
}

pub trait Config: private::Sealed + Serialize + for<'de> Deserialize<'de> {
    /// The Object Store Path for the Config file
    const PATH: &'static str;

    fn persist(
        &self,
        host: impl ToString,
        obj_store: &'_ dyn ObjectStore,
    ) -> impl Future<Output = Result<(), object_store::Error>> + Send {
        let host = host.to_string();
        let payload = PutPayload::from_bytes(serde_json::to_vec(&self).unwrap().into());
        async move {
            obj_store
                .put(&Path::parse(host + Self::PATH).unwrap(), payload)
                .await
                .map(|_| ())
        }
    }

    fn load(
        obj_store: &'_ dyn ObjectStore,
    ) -> impl Future<Output = Result<Self, object_store::Error>> + Send {
        async {
            Ok(serde_json::from_slice(
                &obj_store
                    .get(&Path::parse(Self::PATH).unwrap())
                    .await?
                    .bytes()
                    .await?,
            )
            .unwrap())
        }
    }
}
