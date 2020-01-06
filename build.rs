//use tonic_build;
use prost_build;

fn main() {
    //    tonic_build::compile_protos("proto/delorean/delorean.proto")?;
    prost_build::compile_protos(&["proto/delorean/delorean.proto"], &["proto/delorean/"]).unwrap();
}
