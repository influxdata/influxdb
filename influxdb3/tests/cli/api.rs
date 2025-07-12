use crate::server::TestServer;
use anyhow::{Result, bail};
use assert_cmd::cargo::CommandCargoExt;
use influxdb3_types::http::FieldType;
use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;

// Builder for the 'create database' command
#[derive(Debug)]
pub struct CreateDatabaseQuery<'a> {
    server: &'a TestServer,
    name: String,
}

impl TestServer {
    pub fn create_database(&self, name: impl Into<String>) -> CreateDatabaseQuery<'_> {
        CreateDatabaseQuery {
            server: self,
            name: name.into(),
        }
    }

    fn run_with_options(
        &self,
        commands: Vec<&str>,
        args: &[&str],
        input: Option<&str>,
    ) -> Result<String> {
        let client_addr = self.client_addr();
        let mut command_args = commands.clone();
        command_args.push("--host");
        command_args.push(client_addr.as_str());
        if let Some(token) = self.token() {
            command_args.push("--token");
            command_args.push(token);
        }
        run_cmd_with_result(args, input, command_args)
    }

    pub fn run_regenerate_with_confirmation(
        &self,
        commands: Vec<&str>,
        args: &[&str],
    ) -> Result<String> {
        let client_addr = self.admin_token_recovery_client_addr();
        let mut command_args = commands.clone();
        command_args.push("--host");
        command_args.push(client_addr.as_str());
        run_cmd_with_result(args, Some("yes"), command_args)
    }

    pub fn run(&self, commands: Vec<&str>, args: &[&str]) -> Result<String> {
        self.run_with_options(commands, args, None)
    }

    pub fn run_with_confirmation(&self, commands: Vec<&str>, args: &[&str]) -> Result<String> {
        self.run_with_options(commands, args, Some("yes"))
    }
}

fn run_cmd_with_result(
    args: &[&str],
    input: Option<&str>,
    command_args: Vec<&str>,
) -> std::result::Result<String, anyhow::Error> {
    let mut child_process = Command::cargo_bin("influxdb3")?
        .args(&command_args)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    if let Some(input) = input {
        let input = input.to_string();
        let mut stdin = child_process.stdin.take().expect("failed to open stdin");
        thread::spawn(move || {
            stdin
                .write_all(input.as_bytes())
                .expect("cannot write confirmation msg to stdin");
        });
    }
    let output = child_process.wait_with_output()?;

    if !output.status.success() {
        println!(
            "failed to run influxdb3 {} {}",
            command_args.join(" "),
            args.join(" ")
        );
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    }

    Ok(String::from_utf8(output.stdout)?.trim().into())
}

impl CreateDatabaseQuery<'_> {
    pub fn run(self) -> Result<String> {
        self.server.run(
            vec![
                "create",
                "database",
                "--tls-ca",
                "../testing-certs/rootCA.pem",
            ],
            &[self.name.as_str()],
        )
    }
}
// Builder for the 'create table' command
#[derive(Debug)]
pub struct CreateTableQuery<'a> {
    server: &'a TestServer,
    table_name: String,
    db_name: String,
    tags: Vec<String>,
    fields: Vec<(String, String)>,
}

impl TestServer {
    pub fn create_table(
        &self,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> CreateTableQuery<'_> {
        CreateTableQuery {
            server: self,
            db_name: db_name.into(),
            table_name: table_name.into(),
            tags: Vec::new(),
            fields: Vec::new(),
        }
    }
}

impl CreateTableQuery<'_> {
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    pub fn add_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn with_fields(
        mut self,
        fields: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.fields = fields
            .into_iter()
            .map(|(name, dt)| (name.into(), dt.into()))
            .collect();
        self
    }

    pub fn add_field(mut self, name: impl Into<String>, data_type: impl Into<String>) -> Self {
        self.fields.push((name.into(), data_type.into()));
        self
    }

    pub async fn run_api(self) -> Result<(), influxdb3_client::Error> {
        let fields = self
            .fields
            .into_iter()
            .map(|(name, dt)| {
                (
                    name,
                    match dt.as_ref() {
                        "utf8" => FieldType::Utf8,
                        "bool" => FieldType::Bool,
                        "int64" => FieldType::Int64,
                        "float64" => FieldType::Float64,
                        "uint64" => FieldType::UInt64,
                        _ => panic!("invalid field type"),
                    },
                )
            })
            .collect();

        self.server
            .api_v3_create_table(
                self.db_name.as_str(),
                self.table_name.as_str(),
                self.tags,
                fields,
            )
            .await
    }

    pub fn run(self) -> Result<String> {
        // Convert tags to comma-separated string for --tags argument
        let tags_arg = self.tags.join(",");

        // Convert fields to comma-separated "name:type" string for --fields argument
        let fields_arg = self
            .fields
            .iter()
            .map(|(name, dt)| format!("{name}:{dt}"))
            .collect::<Vec<_>>()
            .join(",");

        let mut args = vec![
            "--database",
            &self.db_name,
            &self.table_name,
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if !self.tags.is_empty() {
            args.push("--tags");
            args.push(&tags_arg);
        }

        if !self.fields.is_empty() {
            args.push("--fields");
            args.push(&fields_arg);
        }

        self.server.run(vec!["create", "table"], &args)
    }
}

// Builder for the 'show databases' command
#[derive(Debug)]
pub struct ShowDatabasesQuery<'a> {
    server: &'a TestServer,
    format: Option<String>,
    show_deleted: bool,
}

impl TestServer {
    pub fn show_databases(&self) -> ShowDatabasesQuery<'_> {
        ShowDatabasesQuery {
            server: self,
            format: None,
            show_deleted: false,
        }
    }
}

impl ShowDatabasesQuery<'_> {
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    pub fn show_deleted(mut self, show: bool) -> Self {
        self.show_deleted = show;
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = Vec::new();

        if let Some(format) = self.format.as_ref() {
            args.push("--format");
            args.push(format);
        }

        if self.show_deleted {
            args.push("--show-deleted");
        }

        args.push("--tls-ca");
        args.push("../testing-certs/rootCA.pem");

        self.server.run(vec!["show", "databases"], &args)
    }
}

// Builder for the 'delete database' command
#[derive(Debug)]
pub struct DeleteDatabaseQuery<'a> {
    server: &'a TestServer,
    name: String,
    hard_delete: Option<String>,
}

impl TestServer {
    pub fn delete_database(&self, name: impl Into<String>) -> DeleteDatabaseQuery<'_> {
        DeleteDatabaseQuery {
            server: self,
            name: name.into(),
            hard_delete: None,
        }
    }
}

impl DeleteDatabaseQuery<'_> {
    pub fn with_hard_delete(mut self, when: impl Into<String>) -> Self {
        self.hard_delete = Some(when.into());
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = vec![self.name.as_str()];

        if let Some(hard_delete) = &self.hard_delete {
            args.push("--hard-delete");
            args.push(hard_delete);
        }

        self.server.run_with_confirmation(
            vec![
                "delete",
                "database",
                "--tls-ca",
                "../testing-certs/rootCA.pem",
            ],
            &args,
        )
    }
}

// Builder for SQL query commands
#[derive(Debug)]
pub struct QuerySqlQuery<'a> {
    server: &'a TestServer,
    database: String,
    query: Option<String>,
    query_file: Option<String>,
    output_file: Option<String>,
    std_in: Option<String>,
}

impl TestServer {
    pub fn query_sql(&self, database: impl Into<String>) -> QuerySqlQuery<'_> {
        QuerySqlQuery {
            server: self,
            database: database.into(),
            query: None,
            query_file: None,
            output_file: None,
            std_in: None,
        }
    }
}

impl QuerySqlQuery<'_> {
    pub fn with_output_file(mut self, file_path: impl Into<String>) -> Self {
        self.output_file = Some(file_path.into());
        self
    }

    pub fn with_sql(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    pub fn with_query_file(mut self, query_file: impl Into<String>) -> Self {
        self.query_file = Some(query_file.into());
        self
    }

    pub fn run(self) -> Result<Value> {
        let mut args = vec![
            "--database",
            &self.database,
            "--format",
            "json",
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];
        if let Some(file_path) = self.output_file.as_ref() {
            args.push("--output");
            args.push(file_path);
        }

        if let Some(query) = self.query.as_ref() {
            args.push(query);
            Ok(serde_json::from_str(
                &self.server.run(vec!["query"], &args)?,
            )?)
        } else if let Some(query_file) = self.query_file.as_ref() {
            args.push("--file");
            args.push(query_file);
            Ok(serde_json::from_str(
                &self.server.run(vec!["query"], &args)?,
            )?)
        } else if let Some(stdin) = self.std_in.as_ref() {
            Ok(serde_json::from_str(&self.server.run_with_options(
                vec!["query"],
                &args,
                Some(stdin.as_str()),
            )?)?)
        } else {
            Ok(serde_json::from_str(
                &self.server.run(vec!["query"], &args)?,
            )?)
        }
    }
}
// Builder for the 'delete table' command
#[derive(Debug)]
pub struct DeleteTableQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    table_name: String,
    hard_delete: Option<String>,
}

impl TestServer {
    pub fn delete_table(
        &self,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> DeleteTableQuery<'_> {
        DeleteTableQuery {
            server: self,
            db_name: db_name.into(),
            table_name: table_name.into(),
            hard_delete: None,
        }
    }
}

impl DeleteTableQuery<'_> {
    pub fn with_hard_delete(mut self, when: impl Into<String>) -> Self {
        self.hard_delete = Some(when.into());
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = vec![
            self.table_name.as_str(),
            "--database",
            self.db_name.as_str(),
        ];

        if let Some(hard_delete) = &self.hard_delete {
            args.push("--hard-delete");
            args.push(hard_delete);
        }

        args.push("--tls-ca");
        args.push("../testing-certs/rootCA.pem");

        self.server
            .run_with_confirmation(vec!["delete", "table"], &args)
    }
}

// Builder for the 'create distinct_cache' command
#[derive(Debug)]
pub struct CreateDistinctCacheQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    table_name: String,
    cache_name: String,
    columns: Vec<String>,
    max_cardinality: Option<usize>,
    max_age: Option<String>,
}

impl TestServer {
    pub fn create_distinct_cache(
        &self,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
        cache_name: impl Into<String>,
    ) -> CreateDistinctCacheQuery<'_> {
        CreateDistinctCacheQuery {
            server: self,
            db_name: db_name.into(),
            table_name: table_name.into(),
            cache_name: cache_name.into(),
            columns: Vec::new(),
            max_cardinality: None,
            max_age: None,
        }
    }
}

impl CreateDistinctCacheQuery<'_> {
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    pub fn add_column(mut self, column: impl Into<String>) -> Self {
        self.columns.push(column.into());
        self
    }

    pub fn with_max_cardinality(mut self, max_cardinality: usize) -> Self {
        self.max_cardinality = Some(max_cardinality);
        self
    }

    pub fn with_max_age(mut self, max_age: impl Into<String>) -> Self {
        self.max_age = Some(max_age.into());
        self
    }

    pub fn run(self) -> Result<String> {
        if self.columns.is_empty() {
            bail!("At least one column must be specified for distinct cache");
        }

        let columns_arg = self.columns.join(",");

        let mut args = vec![
            "--database",
            self.db_name.as_str(),
            "--table",
            self.table_name.as_str(),
            "--columns",
            &columns_arg,
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];
        let max_cardinality = self.max_cardinality.unwrap_or_default();
        let max_cardinality_str = max_cardinality.to_string();

        if max_cardinality > 0 {
            args.push("--max-cardinality");
            args.push(&max_cardinality_str);
        }

        if let Some(max_age) = &self.max_age {
            args.push("--max-age");
            args.push(max_age);
        }

        args.push(self.cache_name.as_str());

        self.server.run(vec!["create", "distinct_cache"], &args)
    }
}

// Builder for the 'delete distinct_cache' command
#[derive(Debug)]
pub struct DeleteDistinctCacheQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    table_name: String,
    cache_name: String,
}

impl TestServer {
    pub fn delete_distinct_cache(
        &self,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
        cache_name: impl Into<String>,
    ) -> DeleteDistinctCacheQuery<'_> {
        DeleteDistinctCacheQuery {
            server: self,
            db_name: db_name.into(),
            table_name: table_name.into(),
            cache_name: cache_name.into(),
        }
    }
}

impl DeleteDistinctCacheQuery<'_> {
    pub fn run(self) -> Result<String> {
        let args = vec![
            "--database",
            self.db_name.as_str(),
            "--table",
            self.table_name.as_str(),
            self.cache_name.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        self.server
            .run(vec!["delete", "distinct_cache"], args.as_slice())
    }
}
// Builder for the 'create trigger' command
#[derive(Debug)]
pub struct CreateTriggerQuery<'a> {
    server: &'a TestServer,
    trigger_name: String,
    db_name: String,
    plugin_filename: String,
    trigger_spec: String,
    trigger_arguments: Vec<String>,
    disabled: bool,
    run_asynchronous: bool,
    error_behavior: Option<String>,
}

impl TestServer {
    pub fn create_trigger(
        &self,
        db_name: impl Into<String>,
        trigger_name: impl Into<String>,
        plugin_filename: impl Into<String>,
        trigger_spec: impl Into<String>,
    ) -> CreateTriggerQuery<'_> {
        CreateTriggerQuery {
            server: self,
            db_name: db_name.into(),
            trigger_name: trigger_name.into(),
            plugin_filename: plugin_filename.into(),
            trigger_spec: trigger_spec.into(),
            trigger_arguments: Vec::new(),
            disabled: false,
            run_asynchronous: false,
            error_behavior: None,
        }
    }
}

impl CreateTriggerQuery<'_> {
    pub fn with_trigger_arguments(
        mut self,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trigger_arguments = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn add_trigger_argument(mut self, arg: impl Into<String>) -> Self {
        self.trigger_arguments.push(arg.into());
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn run_asynchronous(mut self, async_run: bool) -> Self {
        self.run_asynchronous = async_run;
        self
    }

    pub fn error_behavior(mut self, behavior: impl Into<String>) -> Self {
        self.error_behavior = Some(behavior.into());
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = vec![
            self.trigger_name.as_str(),
            "--database",
            self.db_name.as_str(),
            "--plugin-filename",
            self.plugin_filename.as_str(),
            "--trigger-spec",
            self.trigger_spec.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        let trigger_args = self.trigger_arguments.join(",");

        if !self.trigger_arguments.is_empty() {
            args.push("--trigger-arguments");
            args.push(trigger_args.as_str());
        }

        if self.disabled {
            args.push("--disabled");
        }

        if self.run_asynchronous {
            args.push("--run-asynchronous");
        }

        if let Some(behavior) = &self.error_behavior {
            args.push("--error-behavior");
            args.push(behavior);
        }

        self.server.run(vec!["create", "trigger"], args.as_slice())
    }
}
// Builder for the 'enable trigger' command
#[derive(Debug)]
pub struct EnableTriggerQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    trigger_name: String,
}

impl TestServer {
    pub fn enable_trigger(
        &self,
        db_name: impl Into<String>,
        trigger_name: impl Into<String>,
    ) -> EnableTriggerQuery<'_> {
        EnableTriggerQuery {
            server: self,
            db_name: db_name.into(),
            trigger_name: trigger_name.into(),
        }
    }
}

impl EnableTriggerQuery<'_> {
    pub fn run(self) -> Result<String> {
        let args = vec![
            self.trigger_name.as_str(),
            "--database",
            self.db_name.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        self.server.run(vec!["enable", "trigger"], args.as_slice())
    }
}

// Builder for the 'disable trigger' command
#[derive(Debug)]
pub struct DisableTriggerQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    trigger_name: String,
}

impl TestServer {
    pub fn disable_trigger(
        &self,
        db_name: impl Into<String>,
        trigger_name: impl Into<String>,
    ) -> DisableTriggerQuery<'_> {
        DisableTriggerQuery {
            server: self,
            db_name: db_name.into(),
            trigger_name: trigger_name.into(),
        }
    }
}

impl DisableTriggerQuery<'_> {
    pub fn run(self) -> Result<String> {
        let args = vec![
            self.trigger_name.as_str(),
            "--database",
            self.db_name.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        self.server.run(vec!["disable", "trigger"], args.as_slice())
    }
}
// Builder for the 'delete trigger' command
#[derive(Debug)]
pub struct DeleteTriggerQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    trigger_name: String,
    force: bool,
}

impl TestServer {
    pub fn delete_trigger(
        &self,
        db_name: impl Into<String>,
        trigger_name: impl Into<String>,
    ) -> DeleteTriggerQuery<'_> {
        DeleteTriggerQuery {
            server: self,
            db_name: db_name.into(),
            trigger_name: trigger_name.into(),
            force: false,
        }
    }
}

impl DeleteTriggerQuery<'_> {
    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = vec![
            self.trigger_name.as_str(),
            "--database",
            self.db_name.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if self.force {
            args.push("--force");
        }

        self.server.run(vec!["delete", "trigger"], args.as_slice())
    }
}
// Builder for the 'test wal_plugin' command
#[derive(Debug)]
pub struct TestWalPluginQuery<'a> {
    server: &'a TestServer,
    database: String,
    plugin_filename: String,
    line_protocol: Option<String>,
    cache_name: Option<String>,
    input_arguments: Vec<String>,
}

impl TestServer {
    pub fn test_wal_plugin(
        &self,
        database: impl Into<String>,
        plugin_filename: impl Into<String>,
    ) -> TestWalPluginQuery<'_> {
        TestWalPluginQuery {
            server: self,
            database: database.into(),
            plugin_filename: plugin_filename.into(),
            line_protocol: None,
            cache_name: None,
            input_arguments: Vec::new(),
        }
    }
}

impl TestWalPluginQuery<'_> {
    pub fn with_line_protocol(mut self, lp: impl Into<String>) -> Self {
        self.line_protocol = Some(lp.into());
        self
    }

    pub fn with_input_arguments(
        mut self,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.input_arguments = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn add_input_argument(mut self, arg: impl Into<String>) -> Self {
        self.input_arguments.push(arg.into());
        self
    }
    pub fn with_cache_name(mut self, cache_name: impl Into<String>) -> Self {
        self.cache_name = Some(cache_name.into());
        self
    }

    pub fn run(self) -> Result<Value> {
        let mut args = Vec::new();

        args.push("--database");
        args.push(&self.database);

        if let Some(lp) = &self.line_protocol {
            args.push("--lp");
            args.push(lp);
        }
        if let Some(cache_name) = &self.cache_name {
            args.push("--cache-name");
            args.push(cache_name);
        }

        let input_args = self.input_arguments.join(",");

        if !self.input_arguments.is_empty() {
            args.push("--input-arguments");
            args.push(&input_args);
        }

        args.push(&self.plugin_filename);
        args.push("--tls-ca");
        args.push("../testing-certs/rootCA.pem");
        let output = self.server.run(
            vec!["test", "wal_plugin"],
            &args.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
        )?;

        // Parse the output as JSON
        let result = serde_json::from_str(&output).map_err(|e| {
            anyhow::anyhow!("Failed to parse WAL plugin test result as JSON: {}", e)
        })?;

        Ok(result)
    }
}
// Builder for the 'test schedule_plugin' command
#[derive(Debug)]
pub struct TestSchedulePluginQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    plugin_name: String,
    schedule: String,
    cache_name: Option<String>,
    input_arguments: Vec<String>,
}

impl TestServer {
    pub fn test_schedule_plugin(
        &self,
        db_name: impl Into<String>,
        plugin_name: impl Into<String>,
        schedule: impl Into<String>,
    ) -> TestSchedulePluginQuery<'_> {
        TestSchedulePluginQuery {
            server: self,
            db_name: db_name.into(),
            plugin_name: plugin_name.into(),
            schedule: schedule.into(),
            cache_name: None,
            input_arguments: Vec::new(),
        }
    }
}

impl TestSchedulePluginQuery<'_> {
    pub fn with_input_arguments(
        mut self,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.input_arguments = args.into_iter().map(Into::into).collect();
        self
    }
    pub fn with_cache_name(mut self, cache_name: impl Into<String>) -> Self {
        self.cache_name = Some(cache_name.into());
        self
    }

    pub fn add_input_argument(mut self, arg: impl Into<String>) -> Self {
        self.input_arguments.push(arg.into());
        self
    }

    pub fn run(self) -> Result<Value> {
        let mut args = vec![
            "--database",
            self.db_name.as_str(),
            "--schedule",
            self.schedule.as_str(),
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(cache_name) = &self.cache_name {
            args.push("--cache-name");
            args.push(cache_name);
        }
        let input_argument = self.input_arguments.join(",");
        if !self.input_arguments.is_empty() {
            args.push("--input-arguments");
            args.push(input_argument.as_str());
        }

        args.push(self.plugin_name.as_str());

        let result = self.server.run(vec!["test", "schedule_plugin"], &args)?;
        serde_json::from_str(&result).map_err(Into::into)
    }
}
// Builder for the 'package install' command
#[derive(Debug)]
pub struct InstallPackageQuery<'a> {
    server: &'a TestServer,
    packages: Vec<String>,
    requirements_file: Option<String>,
    virtual_env_location: Option<String>,
    package_manager: Option<String>,
}

impl TestServer {
    pub fn install_package(&self) -> InstallPackageQuery<'_> {
        InstallPackageQuery {
            server: self,
            packages: Vec::new(),
            requirements_file: None,
            virtual_env_location: None,
            package_manager: None,
        }
    }
}

impl InstallPackageQuery<'_> {
    pub fn with_packages(mut self, packages: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.packages = packages.into_iter().map(|p| p.into()).collect();
        self
    }

    pub fn add_package(mut self, package: impl Into<String>) -> Self {
        self.packages.push(package.into());
        self
    }

    pub fn with_requirements_file(mut self, requirements_file: impl Into<String>) -> Self {
        self.requirements_file = Some(requirements_file.into());
        self
    }

    pub fn with_virtual_env_location(mut self, virtual_env_location: impl Into<String>) -> Self {
        self.virtual_env_location = Some(virtual_env_location.into());
        self
    }

    pub fn with_package_manager(mut self, package_manager: impl Into<String>) -> Self {
        self.package_manager = Some(package_manager.into());
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = Vec::new();

        // Add the packages if specified
        if !self.packages.is_empty() {
            args.extend(self.packages.iter().map(|p| p.as_str()));
        }

        // Add optional arguments
        if let Some(req_file) = &self.requirements_file {
            args.push("--requirements");
            args.push(req_file);
        }

        if let Some(venv) = &self.virtual_env_location {
            args.push("--virtual-env-location");
            args.push(venv);
        }

        if let Some(pkg_mgr) = &self.package_manager {
            args.push("--package-manager");
            args.push(pkg_mgr);
        }
        args.push("--tls-ca");
        args.push("../testing-certs/rootCA.pem");

        // Run the command
        self.server.run(vec!["package", "install"], &args)
    }
}
// Builder for the 'write' command
#[derive(Debug)]
pub struct WriteQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    line_protocol: Option<String>,
    input_file: Option<String>,
    precision: Option<String>,
}

impl TestServer {
    pub fn write(&self, db_name: impl Into<String>) -> WriteQuery<'_> {
        WriteQuery {
            server: self,
            db_name: db_name.into(),
            line_protocol: None,
            input_file: None,
            precision: None,
        }
    }
}

impl WriteQuery<'_> {
    pub fn with_line_protocol(mut self, line_protocol: impl Into<String>) -> Self {
        self.line_protocol = Some(line_protocol.into());
        self
    }

    pub fn with_file(mut self, file_path: impl Into<String>) -> Self {
        self.input_file = Some(file_path.into());
        self
    }

    pub fn with_precision(mut self, precision: impl Into<String>) -> Self {
        self.precision = Some(precision.into());
        self
    }

    pub fn run(self) -> Result<String> {
        let mut args = vec![
            "--database",
            &self.db_name,
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(precision) = &self.precision {
            args.push("--precision");
            args.push(precision);
        }

        if let Some(file_path) = &self.input_file {
            args.push("--file");
            args.push(file_path);
            return self.server.run(vec!["write"], &args);
        }

        if let Some(lp) = &self.line_protocol {
            // For line protocol provided directly as string
            args.push(lp);
            return self.server.run(vec!["write"], &args);
        }

        // If we get here, there's nothing to write
        bail!("No line protocol provided. Use with_line_protocol() or with_file().")
    }

    pub fn run_with_stdin(self, stdin_input: impl Into<String>) -> Result<String> {
        let mut args = vec![
            "--database",
            &self.db_name,
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(precision) = &self.precision {
            args.push("--precision");
            args.push(precision);
        }

        let input = stdin_input.into();
        self.server
            .run_with_options(vec!["write"], &args, Some(&input))
    }
}

// Base struct for all "show system" subcommands
#[derive(Debug)]
pub struct ShowSystemQuery<'a> {
    server: &'a TestServer,
    db_name: String,
    format: Option<String>,
}

// Specific struct for "table-list" subcommand
#[derive(Debug)]
pub struct ShowSystemTableListQuery<'a> {
    base: ShowSystemQuery<'a>,
}

// Specific struct for "table" subcommand
#[derive(Debug)]
pub struct ShowSystemTableQuery<'a> {
    base: ShowSystemQuery<'a>,
    system_table: String,
    limit: Option<usize>,
    order_by: Option<String>,
    select: Option<String>,
}

// Specific struct for "summary" subcommand
#[derive(Debug)]
pub struct ShowSystemSummaryQuery<'a> {
    base: ShowSystemQuery<'a>,
    limit: Option<usize>,
}

impl TestServer {
    // Entry point for show system commands
    pub fn show_system(&self, db_name: impl Into<String>) -> ShowSystemQuery<'_> {
        ShowSystemQuery {
            server: self,
            db_name: db_name.into(),
            format: None,
        }
    }
}

impl<'a> ShowSystemQuery<'a> {
    // Common method to set output format for all system queries
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    // Branch to table-list subcommand
    pub fn table_list(self) -> ShowSystemTableListQuery<'a> {
        ShowSystemTableListQuery { base: self }
    }

    // Branch to table subcommand
    pub fn table(self, system_table: impl Into<String>) -> ShowSystemTableQuery<'a> {
        ShowSystemTableQuery {
            base: self,
            system_table: system_table.into(),
            limit: None,
            order_by: None,
            select: None,
        }
    }

    // Branch to summary subcommand
    pub fn summary(self) -> ShowSystemSummaryQuery<'a> {
        ShowSystemSummaryQuery {
            base: self,
            limit: None,
        }
    }
}

impl ShowSystemTableListQuery<'_> {
    // Run the table-list command
    pub fn run(self) -> Result<String> {
        let mut args = vec![
            "--database",
            &self.base.db_name,
            "table-list",
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(format) = &self.base.format {
            args.push("--format");
            args.push(format);
        }

        self.base.server.run(vec!["show", "system"], &args)
    }
}

impl ShowSystemTableQuery<'_> {
    // Set limit for table entries
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    // Set order by clause
    pub fn with_order_by(mut self, order_by: impl Into<String>) -> Self {
        self.order_by = Some(order_by.into());
        self
    }

    // Set select fields
    pub fn with_select(mut self, select: impl Into<String>) -> Self {
        self.select = Some(select.into());
        self
    }

    // Run the table command
    pub fn run(self) -> Result<String> {
        let mut args = vec![
            "--database",
            &self.base.db_name,
            "table",
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(format) = &self.base.format {
            args.push("--format");
            args.push(format);
        }

        let limit = self.limit.unwrap_or_default().to_string();

        if self.limit.is_some() {
            args.push("--limit");
            args.push(limit.as_str());
        }

        if let Some(order_by) = &self.order_by {
            args.push("--order-by");
            args.push(order_by);
        }

        if let Some(select) = &self.select {
            args.push("--select");
            args.push(select);
        }

        // System table name is required
        args.push(&self.system_table);

        self.base.server.run(vec!["show", "system"], &args)
    }
}

impl ShowSystemSummaryQuery<'_> {
    // Set limit for summary entries
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    // Run the summary command
    pub fn run(self) -> Result<String> {
        let mut args = vec![
            "--database",
            &self.base.db_name,
            "summary",
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ];

        if let Some(format) = &self.base.format {
            args.push("--format");
            args.push(format);
        }

        let limit = self.limit.unwrap_or_default().to_string();

        if self.limit.is_some() {
            args.push("--limit");
            args.push(limit.as_str());
        }

        self.base.server.run(vec!["show", "system"], &args)
    }
}
