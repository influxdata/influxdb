pub mod float;
pub mod integer;
mod simple8b;
pub mod timestamp;

use crate::storage::StorageError;

/// Types implementing `Encoder` are able to encode themselves into compressed
/// blocks of data.
pub trait Encoder {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError>;
}

impl Encoder for Vec<f64> {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError> {
        float::encode(&self, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Encoder for Vec<i64> {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError> {
        integer::encode(&self, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Encoder for Vec<u64> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Encoder for Vec<&str> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Encoder for Vec<bool> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}
