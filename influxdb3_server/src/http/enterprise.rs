use super::{Error, HttpApi};
use hyper::{Body, Request, Response, StatusCode};
use influxdb3_config::Config;
use influxdb3_types::http::{FileIndexCreateRequest, FileIndexDeleteRequest};
use iox_time::TimeProvider;
use schema::{InfluxColumnType, InfluxFieldType};

impl<T> HttpApi<T>
where
    T: TimeProvider,
{
    pub(crate) async fn enterprise_echo(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let body = req.into_body();
        Response::builder()
            .status(200)
            .body(body)
            .map_err(Into::into)
    }

    pub(crate) async fn configure_file_index_create(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let FileIndexCreateRequest { db, table, columns } = self.read_body_json(req).await?;

        let catalog = self.write_buffer.catalog();
        let db_schema = catalog
            .db_schema(&db)
            .ok_or_else(|| Error::FileIndexDbDoesNotExist(db.clone()))?;
        let table_id = if let Some(table_name) = table {
            Some(
                db_schema
                    .table_name_to_id(table_name.as_str())
                    .ok_or_else(|| super::Error::FileIndexTableDoesNotExist {
                        table_name,
                        db_name: db_schema.name.to_string(),
                    })?,
            )
        } else {
            None
        };
        let _permit = self.common_state.enterprise_config.write_permit().await?;
        match table_id {
            Some(table_id) => {
                let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
                let columns = columns
                    .into_iter()
                    .map(|c| {
                        table_def
                            .column_definition(c.clone())
                            .ok_or_else(|| {
                                Error::FileIndexColumnDoesNotExist(
                                    db.clone(),
                                    table_def.table_name.to_string(),
                                    c.clone(),
                                )
                            })
                            .and_then(|def| match def.data_type {
                                InfluxColumnType::Tag
                                | InfluxColumnType::Field(InfluxFieldType::String) => Ok(def.id),
                                column_type => Err(Error::FileIndexInvalidColumnType {
                                    column_name: c.to_string(),
                                    column_type,
                                }),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.common_state
                    .enterprise_config
                    .add_or_update_columns_for_table(db_schema.id, table_id, columns);
            }
            None => {
                self.common_state
                    .enterprise_config
                    .add_or_update_columns_for_db(db_schema.id, columns);
            }
        }
        self.common_state
            .enterprise_config
            .persist(
                self.write_buffer.catalog().node_id(),
                &self.common_state.object_store,
            )
            .await?;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap())
    }

    pub(crate) async fn configure_file_index_delete(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let FileIndexDeleteRequest { db, table } = self.read_body_json(req).await?;
        let catalog = self.write_buffer.catalog();
        let db_id = catalog
            .db_name_to_id(&db)
            .ok_or_else(|| Error::FileIndexDbDoesNotExist(db.clone()))?;
        let db_schema = catalog
            .db_schema_by_id(&db_id)
            .expect("db schema exists for a db whose id we could look up");
        let _permit = self.common_state.enterprise_config.write_permit().await?;
        match table
            .clone()
            .and_then(|name| db_schema.table_name_to_id(name))
        {
            Some(table_id) => {
                self.common_state
                    .enterprise_config
                    .remove_columns_for_table(&db_id, &table_id)?;
            }
            None => self
                .common_state
                .enterprise_config
                .remove_columns_for_db(&db_id)?,
        }
        self.common_state
            .enterprise_config
            .persist(
                self.write_buffer.catalog().node_id(),
                &self.common_state.object_store,
            )
            .await?;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())?)
    }
}
