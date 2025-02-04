use influxdb3_types::http::{FileIndexCreateRequest, FileIndexDeleteRequest};
use reqwest::Method;

use crate::{Client, Result};

impl Client {
    pub async fn api_v3_configure_file_index_create_or_update(
        &self,
        db: impl Into<String> + Send,
        table: Option<impl Into<String> + Send>,
        columns: Vec<impl Into<String> + Send>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/enterprise/configure/file_index",
                Some(FileIndexCreateRequest {
                    db: db.into(),
                    table: table.map(Into::into),
                    columns: columns.into_iter().map(Into::into).collect(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn api_v3_configure_file_index_delete(
        &self,
        db: impl Into<String> + Send,
        table: Option<impl Into<String> + Send>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/enterprise/configure/file_index",
                Some(FileIndexDeleteRequest {
                    db: db.into(),
                    table: table.map(Into::into),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }
}
