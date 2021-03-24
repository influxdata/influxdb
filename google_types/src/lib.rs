// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![allow(
    unused_imports,
    clippy::redundant_static_lifetimes,
    clippy::redundant_closure,
    clippy::redundant_field_names,
    clippy::clone_on_ref_ptr
)]

mod pb {
    pub mod google {
        pub mod protobuf {
            use std::convert::{TryFrom, TryInto};

            include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));

            impl TryFrom<Duration> for std::time::Duration {
                type Error = std::num::TryFromIntError;

                fn try_from(value: Duration) -> Result<Self, Self::Error> {
                    Ok(std::time::Duration::new(
                        value.seconds.try_into()?,
                        value.nanos.try_into()?,
                    ))
                }
            }

            impl From<std::time::Duration> for Duration {
                fn from(value: std::time::Duration) -> Self {
                    Self {
                        seconds: value.as_secs() as _,
                        nanos: value.subsec_nanos() as _,
                    }
                }
            }
        }
    }
}

pub use pb::google::*;
