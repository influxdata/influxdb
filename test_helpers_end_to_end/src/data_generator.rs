use std::time::SystemTime;

/// Manages a dataset for writing / reading
pub struct DataGenerator {
    ns_since_epoch: i64,
    line_protocol: String,
}

impl DataGenerator {
    pub fn new() -> Self {
        let ns_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should have been after the epoch")
            .as_nanos()
            .try_into()
            .expect("Unable to represent system time");

        let points = vec![
            format!("cpu_load_short,host=server01,region=us-west value=0.64 {ns_since_epoch}"),
            format!(
                "cpu_load_short,host=server01 value=27.99 {}",
                ns_since_epoch + 1
            ),
            format!(
                "cpu_load_short,host=server02,region=us-west value=3.89 {}",
                ns_since_epoch + 2
            ),
            format!(
                "cpu_load_short,host=server01,region=us-east value=1234567.891011 {}",
                ns_since_epoch + 3
            ),
            format!(
                "cpu_load_short,host=server01,region=us-west value=0.000003 {}",
                ns_since_epoch + 4
            ),
            format!(
                "system,host=server03 uptime=1303385i {}",
                ns_since_epoch + 5
            ),
            format!(
                "swap,host=server01,name=disk0 in=3i,out=4i {}",
                ns_since_epoch + 6
            ),
            format!("status active=true {}", ns_since_epoch + 7),
            format!("attributes color=\"blue\" {}", ns_since_epoch + 8),
        ];

        Self {
            ns_since_epoch,
            line_protocol: points.join("\n"),
        }
    }

    /// substitutes "ns" --> ns_since_epoch, ns1-->ns_since_epoch+1, etc
    pub fn substitute_nanos(&self, lines: &[&str]) -> Vec<String> {
        let ns_since_epoch = self.ns_since_epoch;
        let substitutions = vec![
            ("ns0", format!("{ns_since_epoch}")),
            ("ns1", format!("{}", ns_since_epoch + 1)),
            ("ns2", format!("{}", ns_since_epoch + 2)),
            ("ns3", format!("{}", ns_since_epoch + 3)),
            ("ns4", format!("{}", ns_since_epoch + 4)),
            ("ns5", format!("{}", ns_since_epoch + 5)),
            ("ns6", format!("{}", ns_since_epoch + 6)),
        ];

        lines
            .iter()
            .map(|line| {
                let mut line = line.to_string();
                for (from, to) in &substitutions {
                    line = line.replace(from, to);
                }
                line
            })
            .collect()
    }

    /// Get a reference to the data generator's line protocol.
    #[must_use]
    pub fn line_protocol(&self) -> &str {
        self.line_protocol.as_ref()
    }

    /// Get the data generator's ns since epoch.
    #[must_use]
    pub fn ns_since_epoch(&self) -> i64 {
        self.ns_since_epoch
    }

    /// Get the minimum time of the range of this data for querying.
    #[must_use]
    pub fn min_time(&self) -> i64 {
        self.ns_since_epoch
    }

    /// Get the maximum time of the range of this data for querying.
    #[must_use]
    pub fn max_time(&self) -> i64 {
        self.ns_since_epoch + 10
    }
}

impl Default for DataGenerator {
    fn default() -> Self {
        Self::new()
    }
}
