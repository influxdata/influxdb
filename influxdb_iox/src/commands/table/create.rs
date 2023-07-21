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

    /// Partition template: a string of columns or time format each separated by a comma
    /// example: "col1,col2,%Y"
    #[clap(flatten)]
    part_template: PartitionTemplateConfig,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config {
        database,
        table,
        part_template,
    } = config;

    let mut client = influxdb_iox_client::table::Client::new(connection);

    let table = client
        .create_table(&database, &table, part_template.part_template)
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
        let config = Config::try_parse_from([
            "server",
            "database",
            "table",
            "--partition-template",
            "col1,%Y.%j",
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
            ],
        });

        assert_eq!(config.database, "database");
        assert_eq!(config.table, "table");
        assert_eq!(config.part_template.part_template, expected);
    }
}
