use crate::line_parser::Point;

use rocksdb;
use rocksdb::{DB, IteratorMode, WriteBatch};
use byteorder::{ByteOrder, BigEndian, WriteBytesExt, ReadBytesExt};

pub struct Database {
    db: DB,
}

impl Database {
    pub fn new(dir: String) -> Database {
        let db = DB::open_default(dir).unwrap();

        return Database{db}
    }

    // TODO: wire up the org and bucket part of this
    // TODO: wire up series to ID
    // TODO: wire up inverted index creation
    pub fn write_points(&self, _org: &str, _bucket: &str, points: Vec<Point>) -> Result<(), rocksdb::Error> {
        let mut batch = WriteBatch::default();

        for point in points {
            let mut s = point.series.into_bytes();
            s.write_i64::<BigEndian>(point.time);
            let mut val:Vec<u8> = Vec::with_capacity(4);
            val.write_i64::<BigEndian>(point.value);

            batch.put(s, val);
        }

        self.db.write(batch)
    }
}