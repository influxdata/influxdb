use crate::AggregateTSMSchema;
use thiserror::Error;

// Possible validation errors
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Measurement '{measurement}' has a tag and field with the same name: {name}")]
    TagAndFieldSameName { measurement: String, name: String },

    #[error(
        "Measurement '{measurement}' has field '{name}' with multiple types: {:?}",
        types
    )]
    FieldWithMultipleTypes {
        measurement: String,
        name: String,
        types: Vec<String>,
    },
}

pub fn validate_schema(schema: &AggregateTSMSchema) -> Result<(), Vec<ValidationError>> {
    let mut errors: Vec<ValidationError> = vec![];
    for (measurement_name, measurement) in &schema.measurements {
        if let Some(tag_name) = measurement
            .tags
            .keys()
            .find(|&t| measurement.fields.contains_key(t))
        {
            errors.push(ValidationError::TagAndFieldSameName {
                measurement: measurement_name.clone(),
                name: tag_name.clone(),
            });
        }
        if let Some(field) = measurement.fields.values().find(|f| f.types.len() > 1) {
            errors.push(ValidationError::FieldWithMultipleTypes {
                measurement: measurement_name.clone(),
                name: field.name.clone(),
                types: field.types.iter().cloned().collect::<Vec<_>>(),
            });
        }
    }
    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    #[tokio::test]
    async fn good() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        assert_matches!(validate_schema(&schema), Ok(_));
    }

    #[tokio::test]
    async fn tag_and_field_same_name() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "weather": {
              "tags": [
                { "name": "temperature", "values": ["true"] }
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
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        let errors = validate_schema(&schema).expect_err("should fail to validate schema");
        assert_eq!(errors.len(), 1);
        assert_matches!(
            errors.get(0),
            Some(ValidationError::TagAndFieldSameName { .. })
        );
    }

    #[tokio::test]
    async fn field_with_multiple_types() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "weather": {
              "tags": [
                { "name": "location", "values": ["London", "Berlin"] }
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
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        let errors = validate_schema(&schema).expect_err("should fail to validate schema");
        assert_eq!(errors.len(), 1);
        assert_matches!(
            errors.get(0),
            Some(ValidationError::FieldWithMultipleTypes { .. })
        );
    }
}
