type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    tonic_build::compile_protos("proto/delorean/delorean.proto")?;
    Ok(())
}
