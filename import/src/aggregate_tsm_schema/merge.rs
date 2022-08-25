use crate::{
    AggregateTSMField, AggregateTSMMeasurement, AggregateTSMSchema, AggregateTSMSchemaOverride,
    AggregateTSMTag,
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaMergeError {
    /// A schema was found that didn't have the right org and bucket
    #[error("Found org/bucket {0}/{1}; expected {2}/{3}")]
    OrgBucketMismatch(String, String, String, String),

    /// The merge operation found no schemas to merge
    #[error("No schemas found to merge when performing merge operation")]
    NothingToMerge,
}

pub struct SchemaMerger {
    org_id: String,
    bucket_id: String,
    schemas: Vec<AggregateTSMSchema>,
    schema_override: Option<AggregateTSMSchemaOverride>,
}

impl SchemaMerger {
    pub fn new(org_id: String, bucket_id: String, schemas: Vec<AggregateTSMSchema>) -> Self {
        Self {
            org_id,
            bucket_id,
            schemas,
            schema_override: None,
        }
    }

    pub fn with_schema_override(mut self, schema_override: AggregateTSMSchemaOverride) -> Self {
        self.schema_override = Some(schema_override);
        self
    }

    /// Run the merge operation on the list of schemas
    pub fn merge(&self) -> Result<AggregateTSMSchema, SchemaMergeError> {
        // ensure all schemas are for the same org/bucket
        if let Some(s) = self
            .schemas
            .iter()
            .find(|s| s.org_id != self.org_id || s.bucket_id != self.bucket_id)
        {
            return Err(SchemaMergeError::OrgBucketMismatch(
                s.org_id.clone(),
                s.bucket_id.clone(),
                self.org_id.clone(),
                self.bucket_id.clone(),
            ));
        }
        let mut merged_schema = self
            .schemas
            .iter()
            .cloned()
            .reduce(|merged, s| do_merge_schema(&self.org_id, &self.bucket_id, &merged, &s))
            .ok_or(SchemaMergeError::NothingToMerge)?;
        if let Some(schema_override) = &self.schema_override {
            // we have been given config to override parts of the merged schema. usually this comes
            // from a discussion with the customer after attempts to bulk ingest highlighted schema
            // conflicts. using this config file we can A) coerce the data (later, with another
            // tool) and B) modify the schema here before updating it into the IOx catalog, so the
            // coerced data arriving later will match.
            do_schema_override(&mut merged_schema, schema_override);
        }
        Ok(merged_schema)
    }
}

fn do_schema_override(
    merged_schema: &mut AggregateTSMSchema,
    override_schema: &AggregateTSMSchemaOverride,
) {
    for (measurement_name, override_measurement) in &override_schema.measurements {
        // if the override refers to a measurement not in the schema it will be ignored
        if let Some(merged_measurement) = merged_schema.measurements.get_mut(measurement_name) {
            // we only support overrides for field types at this point. later we may support
            // resolving tags/fields with the same name somehow
            for (field_name, override_field) in &override_measurement.fields {
                // if the override refers to a field not in the schema it will be ignored
                if let Some(field) = merged_measurement.fields.get_mut(field_name) {
                    // whatever types were in there, we don't care- replace with the override
                    field.types.clear();
                    field.types.insert(override_field.r#type.clone());
                }
            }
        }
    }
}

// NOTE: assumes org and bucket are the same for both (checked before calling this fn).
//
// this schema merging code is similar to what is used in the IOx router but i decided not to use
// that because:
// - i'm building schemas that, when merged, are potentially bad (e.g. multiple types- in order to
//   identify that very thing).
// - i don't need the underlying parquet metadata struct for this type, it's just an interchange
//   struct to detect schema anomalies. that may change in the future but for now this simple code
//   will suffice.
fn do_merge_schema(
    org_id: &str,
    bucket_id: &str,
    s1: &AggregateTSMSchema,
    s2: &AggregateTSMSchema,
) -> AggregateTSMSchema {
    // start with everything in s1. for-each in s2, either merge or insert
    let mut merged_measurements = s1.measurements.clone();
    s2.measurements.iter().for_each(|s| {
        if let Some(m) = merged_measurements.get_mut(s.0) {
            do_merge_measurement(m, s.1);
        } else {
            // add it
            merged_measurements.insert(s.0.clone(), s.1.clone());
        }
    });
    AggregateTSMSchema {
        org_id: org_id.to_string(),
        bucket_id: bucket_id.to_string(),
        measurements: merged_measurements,
    }
}

fn do_merge_measurement(
    into_measurement: &mut AggregateTSMMeasurement,
    from_measurement: &AggregateTSMMeasurement,
) {
    // merge tags
    from_measurement.tags.values().for_each(|from_tag| {
        if let Some(into_tag) = into_measurement.tags.get(&from_tag.name) {
            let mut new_tag = AggregateTSMTag {
                name: from_tag.name.clone(),
                values: into_tag.values.clone(),
            };
            new_tag.values.extend(from_tag.values.clone().into_iter());
            into_measurement.tags.insert(from_tag.name.clone(), new_tag);
        } else {
            into_measurement
                .tags
                .insert(from_tag.name.clone(), from_tag.clone());
        }
    });
    // merge fields
    from_measurement.fields.values().for_each(|from_field| {
        if let Some(into_field) = into_measurement.fields.get(&from_field.name) {
            let mut new_field = AggregateTSMField {
                name: from_field.name.clone(),
                types: into_field.types.clone(),
            };
            new_field.types.extend(from_field.types.clone().into_iter());
            into_measurement
                .fields
                .insert(from_field.name.clone(), new_field);
        } else {
            into_measurement
                .fields
                .insert(from_field.name.clone(), from_field.clone());
        }
    });
    // ensure sane time ranges have been given
    assert!(
        from_measurement
            .earliest_time
            .le(&from_measurement.latest_time)
            && into_measurement
                .earliest_time
                .le(&into_measurement.latest_time)
    );
    // merge earliest/latest times
    if from_measurement
        .earliest_time
        .lt(&into_measurement.earliest_time)
    {
        into_measurement.earliest_time = from_measurement.earliest_time;
    }
    if from_measurement
        .latest_time
        .gt(&into_measurement.latest_time)
    {
        into_measurement.latest_time = from_measurement.latest_time;
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[tokio::test]
    async fn merge_measurements_adds_if_missing() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        assert_eq!(m1.tags.len(), 2);
        assert_eq!(m1.fields.len(), 2);
    }

    #[tokio::test]
    async fn merge_measurements_merges_tag_with_new_value() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["gadget".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        assert_eq!(m1.tags.len(), 1);
        assert_eq!(m1.fields.len(), 1);
        assert_eq!(
            m1.tags.values().next().unwrap().values,
            HashSet::from([
                "server".to_string(),
                "desktop".to_string(),
                "gadget".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn merge_measurements_merges_tag_with_new_and_old_values() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["gadget".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        assert_eq!(m1.tags.len(), 1);
        assert_eq!(m1.fields.len(), 1);
        assert_eq!(
            m1.tags.values().next().unwrap().values,
            HashSet::from([
                "server".to_string(),
                "desktop".to_string(),
                "gadget".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn merge_measurements_merges_field_with_new_type() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Integer".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        assert_eq!(m1.tags.len(), 1);
        assert_eq!(m1.fields.len(), 1);
        assert_eq!(
            m1.fields.values().next().unwrap().types,
            HashSet::from(["Float".to_string(), "Integer".to_string(),])
        );
    }

    #[tokio::test]
    async fn merge_measurements_merges_field_with_new_and_old_types() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string(), "Integer".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        assert_eq!(m1.tags.len(), 1);
        assert_eq!(m1.fields.len(), 1);
        assert_eq!(
            m1.fields.values().next().unwrap().types,
            HashSet::from(["Float".to_string(), "Integer".to_string(),])
        );
    }

    #[tokio::test]
    async fn merge_measurements_test_time_merge_1() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            // time range falls entirely within the first one
            earliest_time: DateTime::parse_from_rfc3339("2022-04-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-05-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        // result should always be the widest range, i.e. = the first range
        assert_eq!(m1.earliest_time.to_rfc3339(), "2022-01-01T00:00:00+00:00");
        assert_eq!(m1.latest_time.to_rfc3339(), "2022-07-07T06:00:00+00:00");
    }

    #[tokio::test]
    async fn merge_measurements_test_time_merge_2() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-02-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            // time range falls entirely outside the first one
            earliest_time: DateTime::parse_from_rfc3339("2022-06-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        // result should always be the widest range, i.e. = from start of first to end of second
        assert_eq!(m1.earliest_time.to_rfc3339(), "2022-01-01T00:00:00+00:00");
        assert_eq!(m1.latest_time.to_rfc3339(), "2022-07-07T06:00:00+00:00");
    }

    #[tokio::test]
    async fn merge_measurements_test_time_merge_3() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-04-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-05-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            // time range falls before the first one
            earliest_time: DateTime::parse_from_rfc3339("2022-02-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-03-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        // result should always be the widest range, i.e. = from start of second to end of first
        assert_eq!(m1.earliest_time.to_rfc3339(), "2022-02-01T00:00:00+00:00");
        assert_eq!(m1.latest_time.to_rfc3339(), "2022-05-07T06:00:00+00:00");
    }

    #[tokio::test]
    async fn merge_measurements_test_time_merge_4() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-04-01T00:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            // time range starts when the first one ends
            earliest_time: DateTime::parse_from_rfc3339("2022-04-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        // result should always be the widest range, i.e. = from start of first to end of second
        assert_eq!(m1.earliest_time.to_rfc3339(), "2022-01-01T00:00:00+00:00");
        assert_eq!(m1.latest_time.to_rfc3339(), "2022-07-07T06:00:00+00:00");
    }

    #[tokio::test]
    async fn merge_measurements_test_time_merge_5() {
        let mut m1 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "host".to_string(),
                AggregateTSMTag {
                    name: "host".to_string(),
                    values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let m2 = AggregateTSMMeasurement {
            tags: HashMap::from([(
                "sensor".to_string(),
                AggregateTSMTag {
                    name: "sensor".to_string(),
                    values: HashSet::from(["top".to_string(), "bottom".to_string()]),
                },
            )]),
            fields: HashMap::from([(
                "temperature".to_string(),
                AggregateTSMField {
                    name: "temperature".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            // time range is the same as the first one
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        do_merge_measurement(&mut m1, &m2);
        // result should always be the widest range, i.e. = from start of second to end of first
        assert_eq!(m1.earliest_time.to_rfc3339(), "2022-01-01T00:00:00+00:00");
        assert_eq!(m1.latest_time.to_rfc3339(), "2022-07-07T06:00:00+00:00");
    }

    #[tokio::test]
    async fn merge_schema_adds_missing_measurement() {
        let s1 = AggregateTSMSchema {
            org_id: "myorg".to_string(),
            bucket_id: "mybucket".to_string(),
            measurements: HashMap::from([(
                "cpu".to_string(),
                AggregateTSMMeasurement {
                    tags: HashMap::from([(
                        "host".to_string(),
                        AggregateTSMTag {
                            name: "host".to_string(),
                            values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                        },
                    )]),
                    fields: HashMap::from([(
                        "usage".to_string(),
                        AggregateTSMField {
                            name: "usage".to_string(),
                            types: HashSet::from(["Float".to_string()]),
                        },
                    )]),
                    earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00")
                        .unwrap(),
                    latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
                },
            )]),
        };
        let s2 = AggregateTSMSchema {
            org_id: "myorg".to_string(),
            bucket_id: "mybucket".to_string(),
            measurements: HashMap::from([(
                "weather".to_string(),
                AggregateTSMMeasurement {
                    tags: HashMap::from([(
                        "location".to_string(),
                        AggregateTSMTag {
                            name: "location".to_string(),
                            values: HashSet::from(["london".to_string()]),
                        },
                    )]),
                    fields: HashMap::from([(
                        "temperature".to_string(),
                        AggregateTSMField {
                            name: "temperature".to_string(),
                            types: HashSet::from(["Float".to_string()]),
                        },
                    )]),
                    earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00")
                        .unwrap(),
                    latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
                },
            )]),
        };
        let merged = do_merge_schema("myorg", "mybucket", &s1, &s2);
        assert_eq!(merged.org_id, "myorg".to_string());
        assert_eq!(merged.bucket_id, "mybucket".to_string());
        assert_eq!(merged.measurements.len(), 2);
        let mut measurement_names = merged.measurements.keys().cloned().collect::<Vec<_>>();
        measurement_names.sort();
        assert_eq!(
            measurement_names,
            vec!["cpu".to_string(), "weather".to_string()]
        );
    }

    #[tokio::test]
    async fn merge_schema_merges_measurement() {
        let s1 = AggregateTSMSchema {
            org_id: "myorg".to_string(),
            bucket_id: "mybucket".to_string(),
            measurements: HashMap::from([(
                "cpu".to_string(),
                AggregateTSMMeasurement {
                    tags: HashMap::from([(
                        "host".to_string(),
                        AggregateTSMTag {
                            name: "host".to_string(),
                            values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                        },
                    )]),
                    fields: HashMap::from([(
                        "usage".to_string(),
                        AggregateTSMField {
                            name: "usage".to_string(),
                            types: HashSet::from(["Float".to_string()]),
                        },
                    )]),
                    earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00")
                        .unwrap(),
                    latest_time: DateTime::parse_from_rfc3339("2022-04-07T06:00:00+00:00").unwrap(),
                },
            )]),
        };
        let s2 = AggregateTSMSchema {
            org_id: "myorg".to_string(),
            bucket_id: "mybucket".to_string(),
            measurements: HashMap::from([(
                "cpu".to_string(),
                AggregateTSMMeasurement {
                    tags: HashMap::from([(
                        "host".to_string(),
                        AggregateTSMTag {
                            name: "host".to_string(),
                            values: HashSet::from(["gadget".to_string()]),
                        },
                    )]),
                    fields: HashMap::from([(
                        "usage".to_string(),
                        AggregateTSMField {
                            name: "usage".to_string(),
                            types: HashSet::from(["Integer".to_string(), "Float".to_string()]),
                        },
                    )]),
                    earliest_time: DateTime::parse_from_rfc3339("2022-05-01T00:00:00+00:00")
                        .unwrap(),
                    latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
                },
            )]),
        };
        let merged = do_merge_schema("myorg", "mybucket", &s1, &s2);
        assert_eq!(merged.org_id, "myorg".to_string());
        assert_eq!(merged.bucket_id, "mybucket".to_string());
        assert_eq!(merged.measurements.len(), 1);
        let measurement = merged.measurements.get("cpu").unwrap();
        assert_eq!(
            measurement.tags.keys().cloned().collect::<Vec<_>>(),
            vec!["host".to_string()]
        );
        assert_eq!(
            measurement.tags.values().cloned().collect::<Vec<_>>(),
            vec![AggregateTSMTag {
                name: "host".to_string(),
                values: HashSet::from([
                    "server".to_string(),
                    "desktop".to_string(),
                    "gadget".to_string()
                ])
            }]
        );
        assert_eq!(
            measurement.fields.keys().cloned().collect::<Vec<_>>(),
            vec!["usage".to_string()]
        );
        assert_eq!(
            measurement.fields.values().cloned().collect::<Vec<_>>(),
            vec![AggregateTSMField {
                name: "usage".to_string(),
                types: HashSet::from(["Integer".to_string(), "Float".to_string()])
            }]
        );
        assert_eq!(
            measurement.earliest_time.to_rfc3339(),
            "2022-01-01T00:00:00+00:00"
        );
        assert_eq!(
            measurement.latest_time.to_rfc3339(),
            "2022-07-07T06:00:00+00:00"
        );
    }

    #[tokio::test]
    async fn merge_schema_batch() {
        let org = "myorg".to_string();
        let bucket = "mybucket".to_string();
        let merger = SchemaMerger::new(
            org.clone(),
            bucket.clone(),
            vec![
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "cpu".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "host".to_string(),
                                AggregateTSMTag {
                                    name: "host".to_string(),
                                    values: HashSet::from([
                                        "server".to_string(),
                                        "desktop".to_string(),
                                    ]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "usage".to_string(),
                                AggregateTSMField {
                                    name: "usage".to_string(),
                                    types: HashSet::from(["Float".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2021-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2021-03-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "cpu".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "host".to_string(),
                                AggregateTSMTag {
                                    name: "host".to_string(),
                                    values: HashSet::from(["gadget".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "usage".to_string(),
                                AggregateTSMField {
                                    name: "usage".to_string(),
                                    types: HashSet::from(["Integer".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2021-02-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2021-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "weather".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "location".to_string(),
                                AggregateTSMTag {
                                    name: "location".to_string(),
                                    values: HashSet::from(["london".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "temperature".to_string(),
                                AggregateTSMField {
                                    name: "temperature".to_string(),
                                    types: HashSet::from(["Float".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-05-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org,
                    bucket_id: bucket,
                    measurements: HashMap::from([(
                        "weather".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "location".to_string(),
                                AggregateTSMTag {
                                    name: "location".to_string(),
                                    values: HashSet::from(["berlin".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "temperature".to_string(),
                                AggregateTSMField {
                                    name: "temperature".to_string(),
                                    types: HashSet::from(["Integer".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-04-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
            ],
        );
        let json = r#"
        {
          "org_id": "myorg",
          "bucket_id": "mybucket",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop", "gadget"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float", "Integer"] }
              ],
              "earliest_time": "2021-01-01T00:00:00.00Z",
              "latest_time": "2021-07-07T06:00:00.00Z"
            },
            "weather": {
              "tags": [
                { "name": "location", "values": ["london", "berlin"] }
              ],
             "fields": [
                { "name": "temperature", "types": ["Float", "Integer"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let expected: AggregateTSMSchema = json.try_into().unwrap();
        assert_eq!(merger.merge().unwrap(), expected);
    }

    #[tokio::test]
    async fn merge_schema_batch_with_override() {
        let json = r#"
        {
          "measurements": {
            "cpu": {
              "fields": [
                { "name": "usage", "type": "Float" }
              ]
            },
            "weather": {
              "fields": [
                { "name": "temperature", "type": "Float" }
              ]
            }
          }
        }
        "#;
        let override_schema: AggregateTSMSchemaOverride = json.try_into().unwrap();
        let org = "myorg".to_string();
        let bucket = "mybucket".to_string();
        let merger = SchemaMerger::new(
            org.clone(),
            bucket.clone(),
            vec![
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "cpu".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "host".to_string(),
                                AggregateTSMTag {
                                    name: "host".to_string(),
                                    values: HashSet::from([
                                        "server".to_string(),
                                        "desktop".to_string(),
                                    ]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "usage".to_string(),
                                AggregateTSMField {
                                    name: "usage".to_string(),
                                    types: HashSet::from(["Float".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "cpu".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "host".to_string(),
                                AggregateTSMTag {
                                    name: "host".to_string(),
                                    values: HashSet::from(["gadget".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "usage".to_string(),
                                AggregateTSMField {
                                    name: "usage".to_string(),
                                    types: HashSet::from(["Integer".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org.clone(),
                    bucket_id: bucket.clone(),
                    measurements: HashMap::from([(
                        "weather".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "location".to_string(),
                                AggregateTSMTag {
                                    name: "location".to_string(),
                                    values: HashSet::from(["london".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "temperature".to_string(),
                                AggregateTSMField {
                                    name: "temperature".to_string(),
                                    types: HashSet::from(["Float".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
                AggregateTSMSchema {
                    org_id: org,
                    bucket_id: bucket,
                    measurements: HashMap::from([(
                        "weather".to_string(),
                        AggregateTSMMeasurement {
                            tags: HashMap::from([(
                                "location".to_string(),
                                AggregateTSMTag {
                                    name: "location".to_string(),
                                    values: HashSet::from(["berlin".to_string()]),
                                },
                            )]),
                            fields: HashMap::from([(
                                "temperature".to_string(),
                                AggregateTSMField {
                                    name: "temperature".to_string(),
                                    types: HashSet::from(["Integer".to_string()]),
                                },
                            )]),
                            earliest_time: DateTime::parse_from_rfc3339(
                                "2022-01-01T00:00:00+00:00",
                            )
                            .unwrap(),
                            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00")
                                .unwrap(),
                        },
                    )]),
                },
            ],
        )
        .with_schema_override(override_schema);
        let json = r#"
        {
          "org_id": "myorg",
          "bucket_id": "mybucket",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop", "gadget"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            },
            "weather": {
              "tags": [
                { "name": "location", "values": ["london", "berlin"] }
              ],
             "fields": [
                { "name": "temperature", "types": ["Float"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let expected: AggregateTSMSchema = json.try_into().unwrap();
        assert_eq!(merger.merge().unwrap(), expected);
    }

    #[tokio::test]
    async fn override_schema() {
        let json = r#"
        {
          "org_id": "myorg",
          "bucket_id": "mybucket",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop", "gadget"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float", "Integer"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            },
            "weather": {
              "tags": [
                { "name": "location", "values": ["london", "berlin"] }
              ],
             "fields": [
                { "name": "temperature", "types": ["Float", "Integer"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let mut merged_schema: AggregateTSMSchema = json.try_into().unwrap();
        let json = r#"
        {
          "measurements": {
            "cpu": {
              "fields": [
                { "name": "usage", "type": "Float" }
              ]
            },
            "weather": {
              "fields": [
                { "name": "temperature", "type": "Integer" }
              ]
            }
          }
        }
        "#;
        let override_schema: AggregateTSMSchemaOverride = json.try_into().unwrap();
        do_schema_override(&mut merged_schema, &override_schema);
        assert_eq!(
            merged_schema
                .measurements
                .get("cpu")
                .unwrap()
                .fields
                .get("usage")
                .unwrap()
                .types,
            HashSet::from(["Float".to_string()])
        );
        assert_eq!(
            merged_schema
                .measurements
                .get("weather")
                .unwrap()
                .fields
                .get("temperature")
                .unwrap()
                .types,
            HashSet::from(["Integer".to_string()])
        );
    }
}
