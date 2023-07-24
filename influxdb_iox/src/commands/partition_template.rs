use data_types::partition_template::MAXIMUM_NUMBER_OF_TEMPLATE_PARTS;
use generated_types::influxdata::iox::partition_template::v1 as proto;
use influxdb_iox_client::table::generated_types::PartitionTemplate;
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Client Error: Invalid partition template")]
    InvalidPartitionTemplate(),

    #[error("Client Error: Only maximum one time format is allowed")]
    ManyTimeFormat(),

    #[error("Client Error: time cannot be specified as a tag in partition template")]
    TimeAsTag(),
}

/// Partition template: a string of columns or time format each separated by a comma
/// example: "col1,col2,%Y"
#[derive(Debug, clap::Parser, Default, Clone)]
pub struct PartitionTemplateConfig {
    #[clap(action,
        long = "partition-template",
        short = 'p',
        default_value = None,
        value_parser = parse_partition_template,
    )]
    pub part_template: Option<proto::PartitionTemplate>,
}

fn parse_partition_template(s: &str) -> Result<proto::PartitionTemplate, Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::InvalidPartitionTemplate());
    }

    let mut parts = Vec::with_capacity(MAXIMUM_NUMBER_OF_TEMPLATE_PARTS);
    let mut time_format_count = 0;
    for part in s.split(',') {
        if part.is_empty() {
            continue;
        }

        match part {
            // Time format
            "%Y.%j"
            | "%Y"
            | "%Y-%m"
            | "%Y-%m-%d"
            | "%Y-%m-%dT%H"
            | "%Y-%m-%dT%H:%M"
            | "%Y-%m-%dT%H:%M:%S"
            | "%Y-%m-%dT%H:%M:%S.%f"
            | "%Y-%m-%dT%H:%M:%S%.f"
            | "%Y-%m-%dT%H:%M:%SZ"
            | "%Y-%m-%dT%H:%M:%S.%fZ"
            | "%Y-%m-%dT%H:%M:%S%.fZ"
            | "%Y-%m-%dT%H:%M:%S%:z" => {
                time_format_count += 1;
                parts.push(proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat(part.to_string())),
                });

                // onl allow at most one time format
                if time_format_count > 1 {
                    return Err(Error::ManyTimeFormat());
                }
            }
            // time is not allowed in partition template
            "time" => return Err(Error::TimeAsTag()),
            // tag
            _ => parts.push(proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue(part.to_string())),
            }),
        }
    }

    if parts.is_empty() {
        return Err(Error::InvalidPartitionTemplate());
    }

    Ok(PartitionTemplate { parts })
}

#[cfg(test)]
mod tests {
    use crate::commands::partition_template::PartitionTemplateConfig;
    use clap::Parser;

    #[test]
    fn missing_partition_template() {
        let error = PartitionTemplateConfig::try_parse_from(["server", "--partition-template", ""])
            .unwrap_err()
            .to_string();
        print!("{}", error);
        assert!(error.contains("error: invalid value '' for '--partition-template <PART_TEMPLATE>': Client Error: Invalid partition template"));
    }

    // time cannot be a tag
    #[test]
    fn time_as_tag() {
        let error = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "col1,time",
        ])
        .unwrap_err()
        .to_string();

        print!("{}", error);
        assert!(error.contains("error: invalid value 'col1,time' for '--partition-template <PART_TEMPLATE>': Client Error: time cannot be specified as a tag in partition template"));
    }

    // two-time formats
    #[test]
    fn two_time_formats() {
        let error = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "%Y.%j,%Y-%m",
        ])
        .unwrap_err()
        .to_string();

        print!("{}", error);
        assert!(error.contains("error: invalid value '%Y.%j,%Y-%m' for '--partition-template <PART_TEMPLATE>': Client Error: Only maximum one time format is allowed"));
    }

    // valid partition template
    #[test]
    fn valid_partition_template() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "col1,%Y.%j",
        ])
        .unwrap();

        let part_template = actual.part_template.unwrap();
        assert_eq!(part_template.parts.len(), 2);
        assert_eq!(
            part_template.parts[0].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "col1".to_string()
            ))
        );
        assert_eq!(
            part_template.parts[1].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TimeFormat(
                "%Y.%j".to_string()
            ))
        );
    }
}
