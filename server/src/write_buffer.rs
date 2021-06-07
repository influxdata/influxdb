use entry::{Entry, Sequence};

pub trait WriteBuffer: Sync + Send + std::fmt::Debug + 'static {
    fn store_entry(
        &self,
        entry: &Entry,
    ) -> Result<Sequence, Box<dyn std::error::Error + Sync + Send>>;
}

#[derive(Debug)]
pub struct KafkaBuffer {
    conn: String,
}

impl WriteBuffer for KafkaBuffer {
    fn store_entry(
        &self,
        _entry: &Entry,
    ) -> Result<Sequence, Box<dyn std::error::Error + Sync + Send>> {
        unimplemented!()
    }
}

impl KafkaBuffer {
    pub fn new(conn: impl Into<String>) -> Self {
        Self { conn: conn.into() }
    }
}

pub mod test_helpers {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    pub struct MockBuffer {
        pub entries: Arc<Mutex<Vec<Entry>>>,
    }

    impl WriteBuffer for MockBuffer {
        fn store_entry(
            &self,
            entry: &Entry,
        ) -> Result<Sequence, Box<dyn std::error::Error + Sync + Send>> {
            let mut entries = self.entries.lock().unwrap();
            let offset = entries.len() as u64;
            entries.push(entry.clone());

            Ok(Sequence {
                id: 0,
                number: offset,
            })
        }
    }
}
