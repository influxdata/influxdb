pub use iox_objectstore_garbage_collect::{Config, Error};

pub async fn command(config: Config) -> Result<(), Error> {
    iox_objectstore_garbage_collect::main(config).await
}
