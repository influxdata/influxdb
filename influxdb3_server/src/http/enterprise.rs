use super::Error;
use super::HttpApi;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use influxdb3_config::Config;
use influxdb3_config::Index;
use iox_time::TimeProvider;
use serde::Deserialize;

impl<T> HttpApi<T>
where
    T: TimeProvider,
{
    pub async fn enterprise_echo(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        let body = req.into_body();
        Response::builder()
            .status(200)
            .body(body)
            .map_err(Into::into)
    }

    pub async fn configure_file_index_create(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let FileIndexCreateRequest { db, table, columns } = self.read_body_json(req).await?;

        let catalog = self.write_buffer.catalog();
        let db_id = catalog
            .db_name_to_id(&db)
            .ok_or_else(|| Error::FileIndexDbDoesNotExist(db.clone()))?;
        let db_schema = catalog
            .db_schema_by_id(&db_id)
            .expect("schema exists for a db whose id we could look up");
        match table.and_then(|name| db_schema.table_name_to_id(name)) {
            Some(table_id) => {
                let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
                let columns = columns
                    .into_iter()
                    .map(|c| {
                        table_def.column_name_to_id(c.clone()).ok_or_else(|| {
                            Error::FileIndexColumnDoesNotExist(
                                db.clone(),
                                table_def.table_name.to_string(),
                                c,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.common_state
                    .enterprise_config
                    .write()
                    .await
                    .file_index_columns
                    .entry(db_id)
                    // If the db entry exists try to add those columns to the
                    // table or if they don't exist yet create them
                    .and_modify(|idx| {
                        idx.table_columns
                            .entry(table_id)
                            .and_modify(|set| {
                                *set = columns.clone();
                            })
                            .or_insert_with(|| columns.clone());
                    })
                    // If the db entry does not exist create a default Index
                    // and add those columns for that table
                    .or_insert_with(|| {
                        let mut idx = Index::default();
                        idx.table_columns.insert(table_id, columns);

                        idx
                    });

                self.common_state
                    .enterprise_config
                    .read()
                    .await
                    .persist(
                        self.write_buffer.catalog().host_id(),
                        &self.common_state.object_store,
                    )
                    .await?;
            }
            None => {
                self.common_state
                    .enterprise_config
                    .write()
                    .await
                    .file_index_columns
                    .entry(db_id)
                    // if the db entry does exist add these columns for the db
                    .and_modify(|idx| {
                        idx.db_columns = columns.clone().into_iter().map(Into::into).collect();
                    })
                    // if the db entry does not exist create a default Index
                    // and add these columns
                    .or_insert_with(|| {
                        let mut idx = Index::new();
                        idx.db_columns = columns.clone().into_iter().map(Into::into).collect();
                        idx
                    });
                self.common_state
                    .enterprise_config
                    .read()
                    .await
                    .persist(
                        self.write_buffer.catalog().host_id(),
                        &self.common_state.object_store,
                    )
                    .await?;
            }
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap())
    }

    pub async fn configure_file_index_delete(
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
        match table
            .clone()
            .and_then(|name| db_schema.table_name_to_id(name))
        {
            Some(table_id) => {
                match self
                    .common_state
                    .enterprise_config
                    .write()
                    .await
                    .file_index_columns
                    .get_mut(&db_id)
                {
                    Some(Index { table_columns, .. }) => {
                        if table_columns.remove(&table_id).is_none() {
                            return Err(Error::FileIndexTableDoesNotExist(db, table.unwrap()));
                        }
                    }
                    None => return Err(Error::FileIndexDbDoesNotExist(db)),
                }
            }
            None => {
                if self
                    .common_state
                    .enterprise_config
                    .write()
                    .await
                    .file_index_columns
                    .remove(&db_id)
                    .is_none()
                {
                    return Err(Error::FileIndexDbDoesNotExist(db));
                }
            }
        }
        self.common_state
            .enterprise_config
            .read()
            .await
            .persist(
                self.write_buffer.catalog().host_id(),
                &self.common_state.object_store,
            )
            .await?;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())?)
    }
}

/// Request definition for the `POST /api/v3/pro/configure/file_index` API
#[derive(Debug, Deserialize)]
struct FileIndexCreateRequest {
    db: String,
    table: Option<String>,
    columns: Vec<String>,
}

/// Request definition for the `DELETE /api/v3/pro/configure/file_index` API
#[derive(Debug, Deserialize)]
struct FileIndexDeleteRequest {
    db: String,
    table: Option<String>,
}
