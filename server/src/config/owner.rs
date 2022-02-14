//! Code related to managing ownership information in owner.pb

use data_types::server_id::ServerId;
use generated_types::influxdata::iox::management;
use iox_object_store::IoxObjectStore;
use snafu::{ensure, ResultExt, Snafu};
use time::Time;

#[derive(Debug, Snafu)]
pub enum OwnerInfoFetchError {
    #[snafu(display("error loading owner info: {}", source))]
    Loading { source: object_store::Error },

    #[snafu(display("error decoding owner info: {}", source))]
    Decoding {
        source: generated_types::DecodeError,
    },
}

pub(crate) async fn fetch_owner_info(
    iox_object_store: &IoxObjectStore,
) -> Result<management::v1::OwnerInfo, OwnerInfoFetchError> {
    let raw_owner_info = iox_object_store
        .get_owner_file()
        .await
        .context(LoadingSnafu)?;

    generated_types::server_config::decode_database_owner_info(raw_owner_info)
        .context(DecodingSnafu)
}

#[derive(Debug, Snafu)]
pub enum OwnerInfoCreateError {
    #[snafu(display("could not create new owner info file; it already exists"))]
    OwnerFileAlreadyExists,

    #[snafu(display("error creating database owner info file: {}", source))]
    CreatingOwnerFile { source: Box<object_store::Error> },
}

/// Create a new owner info file for this database. Existing content at this location in object
/// storage is an error.
pub(crate) async fn create_owner_info(
    server_id: ServerId,
    server_location: String,
    iox_object_store: &IoxObjectStore,
) -> Result<(), OwnerInfoCreateError> {
    ensure!(
        matches!(
            iox_object_store.get_owner_file().await,
            Err(object_store::Error::NotFound { .. })
        ),
        OwnerFileAlreadyExistsSnafu,
    );

    let owner_info = management::v1::OwnerInfo {
        id: server_id.get_u32(),
        location: server_location,
        transactions: vec![],
    };
    let mut encoded = bytes::BytesMut::new();
    generated_types::server_config::encode_database_owner_info(&owner_info, &mut encoded)
        .expect("owner info serialization should be valid");
    let encoded = encoded.freeze();

    iox_object_store
        .put_owner_file(encoded)
        .await
        .map_err(Box::new)
        .context(CreatingOwnerFileSnafu)?;

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum OwnerInfoUpdateError {
    #[snafu(display("could not fetch existing owner info: {}", source))]
    CouldNotFetch { source: OwnerInfoFetchError },

    #[snafu(display("error updating database owner info file: {}", source))]
    UpdatingOwnerFile { source: object_store::Error },
}

/// Fetch existing owner info, set the `id` and `location`, insert a new entry into the transaction
/// history, and overwrite the contents of the owner file. Errors if the owner info file does NOT
/// currently exist.
pub(crate) async fn update_owner_info(
    new_server_id: Option<ServerId>,
    new_server_location: Option<String>,
    timestamp: Time,
    iox_object_store: &IoxObjectStore,
) -> Result<(), OwnerInfoUpdateError> {
    let management::v1::OwnerInfo {
        id,
        location,
        mut transactions,
    } = fetch_owner_info(iox_object_store)
        .await
        .context(CouldNotFetchSnafu)?;

    let new_transaction = management::v1::OwnershipTransaction {
        id,
        location,
        timestamp: Some(timestamp.date_time().into()),
    };
    transactions.push(new_transaction);

    // TODO: only save latest 100 transactions

    let new_owner_info = management::v1::OwnerInfo {
        // 0 is not a valid server ID, so it indicates "unowned".
        id: new_server_id.map(|s| s.get_u32()).unwrap_or_default(),
        // Owner location is empty when the database is unowned.
        location: new_server_location.unwrap_or_default(),
        transactions,
    };

    let mut encoded = bytes::BytesMut::new();
    generated_types::server_config::encode_database_owner_info(&new_owner_info, &mut encoded)
        .expect("owner info serialization should be valid");
    let encoded = encoded.freeze();

    iox_object_store
        .put_owner_file(encoded)
        .await
        .context(UpdatingOwnerFileSnafu)?;
    Ok(())
}
