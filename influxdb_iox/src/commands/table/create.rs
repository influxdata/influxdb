use crate::commands::{partition_template::PartitionTemplateConfig, table::Result};
use influxdb_iox_client::connection::Connection;

/// Write data into the specified database
#[derive(Debug, clap::Parser, Default, Clone)]
pub struct Config {
    /// The namespace of the table
    #[clap(action)]
    database: String,

    /// The table to be created
    #[clap(action)]
    table: String,

    /// Partition template
    #[clap(flatten)]
    partition_template: PartitionTemplateConfig,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config {
        database,
        table,
        partition_template,
    } = config;

    let partition_template = partition_template.partition_template();

    let mut client = influxdb_iox_client::table::Client::new(connection);

    let table = client
        .create_table(&database, &table, partition_template)
        .await?;
    println!("{}", serde_json::to_string_pretty(&table)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::commands::table::create::Config;
    use clap::Parser;
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use influxdb_iox_client::table::generated_types::PartitionTemplate;

    // valid partition template
    #[test]
    fn valid_partition_template() {
        // Config without partition template
        let config = Config::try_parse_from(["server", "database", "table"]).unwrap();

        assert_eq!(config.database, "database");
        assert_eq!(config.table, "table");

        // Config with partition template
        let config = Config::try_parse_from([
            "server",
            "database",
            "table",
            "--partition-template",
            "{\"parts\": [{\"TagValue\": \"col1\"}, {\"TimeFormat\": \"%Y.%j\"}, {\"TagValue\": \"col2,col3 col4\"}] }",
        ])
        .unwrap();

        let expected = Some(PartitionTemplate {
            parts: vec![
                influxdb_iox_client::namespace::generated_types::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("col1".to_string())),
                },
                influxdb_iox_client::namespace::generated_types::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("%Y.%j".to_string())),
                },
                influxdb_iox_client::namespace::generated_types::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue(
                        "col2,col3 col4".to_string(),
                    )),
                },
            ],
        });

        assert_eq!(config.database, "database");
        assert_eq!(config.table, "table");
        assert_eq!(config.partition_template.partition_template(), expected);
    }
}
