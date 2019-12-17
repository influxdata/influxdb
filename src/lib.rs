extern crate num_cpus;

pub mod line_parser;
pub mod encoders;
pub mod storage;

pub mod delorean {
    include!(concat!(env!("OUT_DIR"), "/delorean.rs"));
}