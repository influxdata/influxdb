use delorean_line_parser::ParsedLine;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// DatabaseRules contains the rules for replicating data, sending data to subscribers, and
/// querying data for a single database.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DatabaseRules {
    // partition_template is used to generate a partition key for each row inserted into the db
    pub partition_template: PartitionTemplate,
    // store_locally if set to true will cause this delorean server to store writes and replicated
    // writes in a local write buffer database. This is step #4 from the diagram.
    pub store_locally: bool,
    // replication is the set of host groups that data should be replicated to. Which host a
    // write goes to within a host group is determined by consistent hashing the partition key.
    // We'd use this to create a host group per availability zone. So you might have 5 availability
    // zones with 2 hosts in each. Replication will ensure that N of those zones get a write. For
    // each zone, only a single host needs to get the write. Replication is for ensuring a write
    // exists across multiple hosts before returning success. Its purpose is to ensure write
    // durability, rather than write availability for query (this is covered by subscriptions).
    pub replication: Vec<HostGroupId>,
    // replication_count is the minimum number of host groups to replicate a write to before
    // success is returned. This can be overridden on a per request basis. Replication will
    // continue to write to the other host groups in the background.
    pub replication_count: u8,
    // replication_queue_max_size is used to determine how far back replication can back up before
    // either rejecting writes or dropping missed writes. The queue is kept in memory on a per
    // database basis. A queue size of zero means it will only try to replicate synchronously and
    // drop any failures.
    pub replication_queue_max_size: usize,
    // subscriptions are used for query servers to get data via either push or pull as it arrives.
    // They are separate from replication as they have a different purpose. They're for query
    // servers or other clients that want to subscribe to some subset of data being written in.
    // This could either be specific partitions, ranges of partitions, tables, or rows matching
    // some predicate. This is step #3 from the diagram.
    pub subscriptions: Vec<Subscription>,

    // query local is set to true if this server should answer queries from either its local
    // write buffer and/or read-only partitions that it knows about. If set to true, results
    // will be merged with any others from the remote goups or read only partitions.
    pub query_local: bool,
    // primary_query_group should be set to a host group if remote servers should be issued
    // queries for this database. All hosts in the group should be queried with this server
    // acting as the coordinator that merges results together. If a specific host in the group
    // is unavailable, another host in the same position from a secondary group should be
    // queried. For example, if we've partitioned the data in this DB into 4 partitions and
    // we are replicating the data across 3 availability zones. Say we have 4 hosts in each
    // of those AZs, thus they each have 1 partition. We'd set the primary group to be the 4
    // hosts in the same AZ as this one. And the secondary groups as the hosts in the other
    // 2 AZs.
    pub primary_query_group: Option<HostGroupId>,
    pub secondary_query_groups: Vec<HostGroupId>,

    // read_only_partitions are used when a server should answer queries for partitions that
    // come from object storage. This can be used to start up a new query server to handle
    // queries by pointing it at a collection of partitions and then telling it to also pull
    // data from the replication servers (writes that haven't been snapshotted into a partition).
    pub read_only_partitions: Vec<PartitionId>,
}

impl DatabaseRules {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        self.partition_template.partition_key(line, default_time)
    }
}

/// PartitionTemplate is used to compute the partition key of each row that gets written. It
/// can consist of the table name, a column name and its value, a formatted time, or a string
/// column and regex captures of its value. For columns that do not appear in the input row
/// a blank value is output.
///
/// The key is constructed in order of the template parts, thus ordering changes what partition
/// key is generated.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PartitionTemplate {
    parts: Vec<TemplatePart>,
}

impl PartitionTemplate {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        let parts: Vec<_> = self
            .parts
            .iter()
            .map(|p| match p {
                TemplatePart::Table => line.series.measurement.to_string(),
                TemplatePart::Column(column) => match line.tag_value(&column) {
                    Some(v) => format!("{}_{}", column, v.to_string()),
                    None => match line.field_value(&column) {
                        Some(v) => format!("{}_{}", column, v),
                        None => "".to_string(),
                    },
                },
                TemplatePart::TimeFormat(format) => match line.timestamp {
                    Some(t) => Utc.timestamp_nanos(t).format(&format).to_string(),
                    None => default_time.format(&format).to_string(),
                },
                _ => unimplemented!(),
            })
            .collect();

        Ok(parts.join("-"))
    }
}

/// TemplatePart specifies what part of a row should be used to compute this part of a partition key.
#[derive(Debug, Serialize, Deserialize)]
pub enum TemplatePart {
    Table,
    Column(String),
    TimeFormat(String),
    RegexCapture(RegexCapture),
    StrftimeColumn(StrftimeColumn),
}

/// RegexCapture is for pulling parts of a string column into the partition key.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegexCapture {
    column: String,
    regex: String,
}

/// StrftimeColumn can be used to create a time based partition key off some column other than
/// the builtin "time" column.
#[derive(Debug, Serialize, Deserialize)]
pub struct StrftimeColumn {
    column: String,
    format: String,
}

/// PartitionId is the object storage identifier for a specific partition. It should be a
/// path that can be used against an object store to locate all the files and subdirectories
/// for a partition. It takes the form of /<writer ID>/<database>/<partition key>/
pub type PartitionId = String;
pub type WriterId = String;

#[derive(Debug, Serialize, Deserialize)]
enum SubscriptionType {
    Push,
    Pull,
}

/// Subscription represent a group of hosts that want to either receive data pushed
/// as it arrives or want to pull it periodically. The subscription has a matcher
/// that is used to determine what data will match it, and an optional queue for
/// storing matched writes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Subscription {
    name: String,
    host_group: HostGroupId,
    subscription_type: SubscriptionType,
    matcher: Matcher,
    // max_queue_size is used for subscriptions that can potentially get queued up either for
    // pulling later, or in the case of a temporary outage for push subscriptions.
    max_queue_size: usize,
}

/// Matcher specifies the rule against the table name and/or a predicate
/// against the row to determine if it matches the write rule.
#[derive(Debug, Serialize, Deserialize)]
struct Matcher {
    #[serde(flatten)]
    tables: MatchTables,
    // TODO: make this work with delorean_storage::Predicate
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate: Option<String>,
}

/// MatchTables looks at the table name of a row to determine if it should
/// match the rule.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum MatchTables {
    #[serde(rename = "*")]
    All,
    Table(String),
    Regex(String),
}

type HostGroupId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct HostGroup {
    id: HostGroupId,
    // hosts is a vec of connection strings for remote hosts
    hosts: Vec<String>,
}

mod tests {
    use super::*;
    use delorean_line_parser::parse_lines;

    #[allow(dead_code)]
    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[allow(dead_code)]
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn partition_key_with_table() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Table],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("cpu", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_int_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("foo_1", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_float_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1.1 10");
        assert_eq!(
            "foo_1.1",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_string_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=\"asdf\" 10");
        assert_eq!(
            "foo_asdf",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_bool_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("bar".to_string())],
        };

        let line = parse_line("cpu bar=true 10");
        assert_eq!(
            "bar_true",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_tag_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("region".to_string())],
        };

        let line = parse_line("cpu,region=west usage_user=23.2 10");
        assert_eq!(
            "region_west",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_missing_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("not_here".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 10");
        assert_eq!("", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_time() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 1602338097000000000");
        assert_eq!(
            "2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_default_time() -> Result {
        let format_string = "%Y-%m-%d %H:%M:%S";
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat(format_string.to_string())],
        };

        let default_time = Utc::now();
        let line = parse_line("cpu,foo=asdf bar=true");
        assert_eq!(
            default_time.format(format_string).to_string(),
            template.partition_key(&line, &default_time).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_many_parts() -> Result {
        let template = PartitionTemplate {
            parts: vec![
                TemplatePart::Table,
                TemplatePart::Column("region".to_string()),
                TemplatePart::Column("usage_system".to_string()),
                TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string()),
            ],
        };

        let line = parse_line(
            "cpu,host=a,region=west usage_user=22.1,usage_system=53.1 1602338097000000000",
        );
        assert_eq!(
            "cpu-region_west-usage_system_53.1-2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[allow(dead_code)]
    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    #[allow(dead_code)]
    fn parse_line(line: &str) -> ParsedLine<'_> {
        parsed_lines(line).pop().unwrap()
    }
}
