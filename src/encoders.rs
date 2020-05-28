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

/// Types implementing `Decoder` are able to decode themselves from compressed
/// blocks of vectors of values.
pub trait Decoder<T> {
    fn decode(dst: &mut Vec<T>, src: Vec<u8>) -> Result<(), StorageError>;
}

impl Decoder<f64> for Vec<f64> {
    fn decode(dst: &mut Vec<f64>, src: Vec<u8>) -> Result<(), StorageError> {
        float::decode(&src, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Decoder<i64> for Vec<i64> {
    fn decode(dst: &mut Vec<i64>, src: Vec<u8>) -> Result<(), StorageError> {
        integer::decode(&src, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Decoder<u64> for Vec<u64> {
    fn decode(_: &mut Vec<u64>, _: Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Decoder<&str> for Vec<&str> {
    fn decode(_: &mut Vec<&str>, _: Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Decoder<bool> for Vec<bool> {
    fn decode(_: &mut Vec<bool>, _: Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}
