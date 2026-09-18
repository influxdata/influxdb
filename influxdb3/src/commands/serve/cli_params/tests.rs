use clap::CommandFactory;
use hashbrown::HashSet;

use crate::commands::serve::Config;

use super::*;

#[test]
fn test_sensitive_params_are_redacted() {
    let mut params = HashMap::new();
    for sensitive in SENSITIVE_PARAMS {
        params.insert(sensitive.to_string(), "un-redacted".to_string());
    }
    let result = capture_cli_params(params);
    let parsed = serde_json::from_str::<HashMap<String, String>>(&result).unwrap();
    assert_eq!(
        parsed.len(),
        SENSITIVE_PARAMS.len(),
        "expected there to be {n} parsed entries",
        n = SENSITIVE_PARAMS.len()
    );
    for sensitive in SENSITIVE_PARAMS {
        assert_eq!(
            parsed.get(*sensitive).unwrap(),
            REDACTED_STR,
            "expected {REDACTED_STR} for '{sensitive}' argument"
        );
    }
}

/// Extract all argument IDs from a Command recursively
fn extract_all_arg_ids(cmd: &clap::Command, args: &mut HashSet<String>) {
    for arg in cmd.get_arguments() {
        let id = arg.get_id().as_str();

        // Skip help and version which are always present
        if id == "help" || id == "version" || id == "help-all" {
            continue;
        }

        // Get the display name (long form or short form or id)
        let display_name = if let Some(long) = arg.get_long() {
            long.to_string()
        } else if let Some(short) = arg.get_short() {
            format!("{}", short)
        } else {
            id.to_string()
        };

        args.insert(display_name);
    }

    // Recursively process subcommands
    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() != "help" {
            extract_all_arg_ids(subcmd, args);
        }
    }
}

#[test]
fn test_all_config_params_categorized() {
    // Use the module-level constants - no need to redefine them here
    // Get all arguments from the Config command
    let cmd = Config::command();
    let mut discovered_args = HashSet::new();
    extract_all_arg_ids(
        cmd.get_subcommands()
            .find(|c| c.get_name() == "serve")
            .unwrap_or(&cmd),
        &mut discovered_args,
    );

    // If there are no serve subcommand, check the root
    if discovered_args.is_empty() {
        extract_all_arg_ids(&cmd, &mut discovered_args);
    }

    let mut uncategorized = Vec::new();

    for arg in &discovered_args {
        let is_in_non_sensitive_list = NON_SENSITIVE_PARAMS.contains(&arg.as_str());
        let is_in_sensitive_list = SENSITIVE_PARAMS.contains(&arg.as_str());

        if !is_in_non_sensitive_list && !is_in_sensitive_list {
            // Check if it might be caught by substring matching in is_sensitive function
            if !is_sensitive(arg) {
                uncategorized.push(arg.clone());
            }
        }
    }

    if !uncategorized.is_empty() {
        panic!(
            "The following CLI parameters are not categorized as either sensitive or \
            non-sensitive:\n{}\n\n\
            Please add them to either NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants \
            at the module level.",
            uncategorized.join("\n")
        );
    }

    let mut needlessly_categorized = Vec::new();

    for arg in NON_SENSITIVE_PARAMS.iter().chain(SENSITIVE_PARAMS) {
        let is_discovered = discovered_args.contains(*arg);
        if !is_discovered {
            needlessly_categorized.push(arg.to_owned());
        }
    }

    if !needlessly_categorized.is_empty() {
        panic!(
            "The following CLI parameters were set as either sensitive or non-sensitive \
            but were not discovered in the actual command:\n{}\n\n\
            Please remove them from the NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants.",
            needlessly_categorized.join("\n")
        );
    }
}

/// Extract all public (non-hidden) argument long names from a Command recursively.
/// Returns a HashSet of option names (without the leading "--").
fn extract_public_cli_options(cmd: &clap::Command, args: &mut HashSet<String>) {
    for arg in cmd.get_arguments() {
        // Skip help and version which are always present
        let id = arg.get_id().as_str();
        if id == "help" || id == "version" || id == "help-all" {
            continue;
        }

        // Skip hidden arguments - these are internal/test-only and shouldn't
        // be in the config file
        if arg.is_hide_set() {
            continue;
        }

        // Only include arguments that have a long form (these are the ones
        // that can be configured via TOML config)
        if let Some(long) = arg.get_long() {
            args.insert(long.to_string());
        }
    }

    // Recursively process subcommands
    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() != "help" {
            extract_public_cli_options(subcmd, args);
        }
    }
}

/// Parse the core config file and extract all documented option names.
/// Options are in the format: #option-name="value" or #option-name=value
fn extract_config_file_options(config_content: &str) -> HashSet<String> {
    let mut options = HashSet::new();

    for line in config_content.lines() {
        let line = line.trim();

        // Skip empty lines and comment-only lines (lines starting with # followed
        // by space or that don't contain '=')
        if line.is_empty() || !line.contains('=') {
            continue;
        }

        // Match lines like: #option-name="value" or option-name="value"
        // The '#' prefix indicates a commented-out (default) option
        let line = line.strip_prefix('#').unwrap_or(line);

        // Extract the option name (everything before the '=')
        if let Some(eq_pos) = line.find('=') {
            let option_name = line[..eq_pos].trim();

            // Only include valid option names (kebab-case identifiers)
            if !option_name.is_empty()
                && option_name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                options.insert(option_name.to_string());
            }
        }
    }

    options
}

/// Options (long form) that are intentionally excluded from the config file.
/// These are either:
/// - Deprecated and will be removed
/// - Have special handling that doesn't fit the config file model
/// - Are handled by the launcher itself (not passed to influxdb3)
/// - CLI-only debugging/interactive flags
const CONFIG_EXCLUDED_OPTIONS: &[&str] = &[
    // Verbose flag - typically used only on command line for debugging
    "verbose",
    // Enterprise-only option that errors when used with Core OSS
    "cluster-id",
];

/// Options that exist in the config file but not in CLI.
/// These are typically options that are:
/// - Renamed in CLI (old name kept in config for backwards compatibility)
/// - Translated by the launcher to a different CLI option
/// - Other runtime options not part of serve subcommand's Config
const CLI_EXCLUDED_OPTIONS: &[&str] = &[
    // Renamed to num-datafusion-threads in CLI
    "datafusion-num-threads",
    // Top-level IO runtime options - these are valid CLI options but
    // defined using the tokio_rt_config! macro outside of serve::Config.
    // The test only introspects serve::Config, so these appear as
    // "missing" from CLI.
    "io-runtime-type",
    "io-runtime-disable-lifo-slot",
    "io-runtime-event-interval",
    "io-runtime-global-queue-interval",
    "io-runtime-max-blocking-threads",
    "io-runtime-max-io-events-per-tick",
    "io-runtime-thread-keep-alive",
    "io-runtime-thread-priority",
    "num-io-threads",
];

#[test]
fn test_config_file_cli_option_drift() {
    use crate::commands::serve::Config;

    // Path to the core config file (relative to workspace root)
    let config_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.circleci/packages/influxdb3/fs/usr/share/influxdb3/influxdb3-core.conf"
    );

    // Read the config file
    let config_content = std::fs::read_to_string(config_path).unwrap_or_else(|e| {
        panic!(
            "Failed to read config file at {}: {}\n\
             This test verifies that CLI options match the config file template.",
            config_path, e
        )
    });

    // Extract options from CLI (using clap introspection)
    let cmd = Config::command();
    let mut cli_options = HashSet::new();
    extract_public_cli_options(&cmd, &mut cli_options);

    // Extract options from config file
    let config_options = extract_config_file_options(&config_content);

    // Find options in CLI but not in config file
    let mut missing_from_config: Vec<_> = cli_options
        .iter()
        .filter(|opt| {
            !config_options.contains(*opt) && !CONFIG_EXCLUDED_OPTIONS.contains(&opt.as_str())
        })
        .cloned()
        .collect();
    missing_from_config.sort();

    // Find options in config file but not in CLI
    let mut missing_from_cli: Vec<_> = config_options
        .iter()
        .filter(|opt| !cli_options.contains(*opt) && !CLI_EXCLUDED_OPTIONS.contains(&opt.as_str()))
        .cloned()
        .collect();
    missing_from_cli.sort();

    // Build error message if there's drift
    let mut error_msg = String::new();

    if !missing_from_config.is_empty() {
        error_msg.push_str(&format!(
            "\n\nCLI options missing from config file ({}):\n",
            config_path
        ));
        for opt in &missing_from_config {
            error_msg.push_str(&format!("  - {}\n", opt));
        }
        error_msg.push_str("\nTo fix: Add these options to the config file template, or add them to\nCONFIG_EXCLUDED_OPTIONS if they should be excluded. Until influxdb3 supports\nTOML configuration natively, when adding to the config file template, you\nmust also consider that the launcher maps options to the corresponding\nenvironment variable for the option by using this pattern:\n`INFLUXDB3_ + key.replace('-', '_').upper()`. If the new option doesn't\nfollow this pattern (ie, all 'INFLUXDB3_ENTERPRISE_...' env vars), update\nTOML_KEY_ENVVAR in `influxdb3-launcher` to include your new mapping.");
    }

    if !missing_from_cli.is_empty() {
        error_msg.push_str(&format!(
            "\n\nConfig file options not found in CLI ({}):\n",
            config_path
        ));
        for opt in &missing_from_cli {
            error_msg.push_str(&format!("  - {}\n", opt));
        }
        error_msg.push_str("\nTo fix: Remove these options from the config file, or add them to\nCLI_EXCLUDED_OPTIONS if they're handled specially.");
    }

    if !error_msg.is_empty() {
        panic!("Config file and CLI options are out of sync!{}", error_msg);
    }
}

/// Walk every serve arg (hidden ones included) and fail if any help or
/// long-help text points at an internal `::default` (or other `::`-typed
/// constructor: `::new`, `::from`) implementation — i.e. names the internal
/// component that produces a value instead of stating the effective default
/// and its user-visible effect. This is the deterministic backstop for the
/// CLI-surface review rule in
/// `.github/instructions/cli-surface.instructions.md`; Copilot code review
/// filters low-confidence comments and is non-deterministic, so it cannot be
/// relied on to catch these.
///
/// The pattern is deliberately narrow (`Type::default`/`::new`/`::from`) so
/// that legitimate user-facing prose survives: `RUST_LOG`-style filter
/// examples (`hyper::proto=info`), documented default expressions
/// (`usize::MAX >> 3`), and similar are not internal-consumer references.
#[test]
fn help_text_has_no_internal_default_refs() {
    // A `Type::default` / `Type::new` / `Type::from` impl pointer.
    let internal_ref =
        regex::Regex::new(r"[A-Za-z_][A-Za-z0-9_]*::(default|new|from)\b").expect("valid regex");

    let mut offenders = Vec::new();
    collect_internal_help_refs(&Config::command(), &internal_ref, &mut offenders);

    assert!(
        offenders.is_empty(),
        "The following CLI flags point at an internal constructor/`::default` \
         impl in their help text. Restate the effective default value and its \
         user-visible effect instead of naming the internal type:\n{}",
        offenders.join("\n")
    );
}

/// Recursively collect `--flag: matched-text` entries for any arg whose help
/// or long-help matches `internal_ref`.
fn collect_internal_help_refs(
    cmd: &clap::Command,
    internal_ref: &regex::Regex,
    offenders: &mut Vec<String>,
) {
    for arg in cmd.get_arguments() {
        let id = arg.get_id().as_str();
        if id == "help" || id == "version" || id == "help-all" {
            continue;
        }
        let name = arg
            .get_long()
            .map(str::to_owned)
            .unwrap_or_else(|| id.to_owned());
        for help in [arg.get_long_help(), arg.get_help()].into_iter().flatten() {
            let help = help.to_string();
            if let Some(m) = internal_ref.find(&help) {
                offenders.push(format!("--{name}: `{}`", m.as_str()));
            }
        }
    }
    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() != "help" {
            collect_internal_help_refs(subcmd, internal_ref, offenders);
        }
    }
}

/// Parse the launcher's `TOML_KEY_ENVVAR` table: `section -> (toml key -> env name)`.
///
/// The table is a Python dict literal with one `"key": "ENV",` entry per
/// line, grouped under `"common"`, `"core"` and `"enterprise"` sections.
fn parse_launcher_env_table(launcher: &str) -> HashMap<String, HashMap<String, String>> {
    let mut table: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut section: Option<String> = None;
    let mut in_table = false;
    for line in launcher.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("TOML_KEY_ENVVAR") {
            in_table = true;
            continue;
        }
        if !in_table {
            continue;
        }
        // The table ends at the first unindented closing brace.
        if line == "}" {
            break;
        }
        if trimmed.starts_with('#') {
            continue;
        }
        let Some((key, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let key = key.trim().trim_matches('"');
        let rest = rest.trim();
        if rest.starts_with('{') {
            section = Some(key.to_string());
            continue;
        }
        let env = rest.trim_end_matches(',').trim_matches('"');
        let section = section
            .as_ref()
            .unwrap_or_else(|| panic!("launcher table entry {key} outside any section"));
        table
            .entry(section.clone())
            .or_default()
            .insert(key.to_string(), env.to_string());
    }
    assert!(in_table, "TOML_KEY_ENVVAR table not found in launcher");
    table
}

/// `.conf` keys kept as deprecated aliases of a renamed flag. The launcher
/// must export the env name of the flag they alias.
const CONF_KEY_ALIASES: &[(&str, &str)] = &[("datafusion-num-threads", "num-datafusion-threads")];

fn canonical_conf_key(key: &str) -> &str {
    CONF_KEY_ALIASES
        .iter()
        .find(|(alias, _)| *alias == key)
        .map_or(key, |(_, canonical)| canonical)
}

/// The env name the launcher exports for a `.conf` key, mirroring
/// `read_config_toml` in the launcher: explicit table entry (flavor
/// overrides common), else `INFLUXDB3_` + SCREAMING_SNAKE of the key.
fn launcher_env_for(
    table: &HashMap<String, HashMap<String, String>>,
    flavor: &str,
    key: &str,
) -> String {
    table
        .get(flavor)
        .and_then(|m| m.get(key))
        .or_else(|| table.get("common").and_then(|m| m.get(key)))
        .cloned()
        .unwrap_or_else(|| format!("INFLUXDB3_{}", key.replace('-', "_").to_uppercase()))
}

/// Collect `long name -> env name` for every clap arg (hidden included: the
/// launcher exports whatever the `.conf` template names, hidden or not).
fn extract_cli_env_names(cmd: &clap::Command, out: &mut HashMap<String, Option<String>>) {
    for arg in cmd.get_arguments() {
        if let Some(long) = arg.get_long() {
            out.insert(
                long.to_string(),
                arg.get_env().map(|e| e.to_string_lossy().into_owned()),
            );
        }
    }
    for subcmd in cmd.get_subcommands() {
        extract_cli_env_names(subcmd, out);
    }
}

/// Every env name any argument in the tree binds. Unlike
/// [`extract_cli_env_names`], nothing is keyed by long name, so two commands
/// sharing a long (e.g. `--node-id` on `serve` and elsewhere) cannot shadow
/// each other's binding.
fn extract_bound_env_names(cmd: &clap::Command, out: &mut HashSet<String>) {
    for arg in cmd.get_arguments() {
        if let Some(env) = arg.get_env() {
            out.insert(env.to_string_lossy().into_owned());
        }
    }
    for subcmd in cmd.get_subcommands() {
        extract_bound_env_names(subcmd, out);
    }
}

/// Every `.conf` template key must reach the binary through the launcher
/// under the env name clap actually binds for that flag. A launcher entry
/// that exports a deprecated alias makes every packaged start log an
/// `env_compat` deprecation warning for a variable the user never set
/// (#5465); one that exports an unbound name silently drops the setting
/// (#4816).
#[test]
fn test_launcher_env_names_match_cli() {
    use crate::commands::serve::Config;
    use influxdb3_startup::env_compat::ENV_ALIASES;

    let launcher_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.circleci/packages/influxdb3/fs/usr/lib/influxdb3/influxdb3-launcher"
    );
    let config_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.circleci/packages/influxdb3/fs/usr/share/influxdb3/influxdb3-core.conf"
    );
    let flavor = "core";

    let launcher = std::fs::read_to_string(launcher_path)
        .unwrap_or_else(|e| panic!("Failed to read launcher at {launcher_path}: {e}"));
    let config = std::fs::read_to_string(config_path)
        .unwrap_or_else(|e| panic!("Failed to read config file at {config_path}: {e}"));

    let table = parse_launcher_env_table(&launcher);
    let mut cli_env = HashMap::new();
    extract_cli_env_names(&Config::command(), &mut cli_env);
    let legacy: HashMap<&str, &str> = ENV_ALIASES.iter().map(|(new, old)| (*old, *new)).collect();

    let mut keys: Vec<_> = extract_config_file_options(&config).into_iter().collect();
    keys.sort();
    let mut errors = Vec::new();
    for key in keys {
        let exported = launcher_env_for(&table, flavor, &key);
        let Some(bound) = cli_env.get(canonical_conf_key(&key)) else {
            // Covered by test_config_file_cli_option_drift.
            continue;
        };
        // A flag may bind a legacy env primary that ENV_ALIASES maps a
        // canonical INFLUXDB3_ name onto (the trogging/trace_exporters
        // exception in docs/cli-conventions.md); the launcher must export
        // the canonical name, which env_compat copies onto the primary.
        let expected = bound
            .as_deref()
            .map(|b| legacy.get(b).copied().unwrap_or(b));
        match expected {
            Some(expected) if expected == exported => {}
            Some(expected) => match legacy.get(exported.as_str()) {
                Some(canonical) => errors.push(format!(
                    "  - {key}: launcher exports {exported}, a deprecated alias of \
                     {canonical} (every packaged start logs a deprecation warning)"
                )),
                None => errors.push(format!(
                    "  - {key}: launcher exports {exported}, but the flag binds {expected} \
                     (the setting is silently ignored, or read through a clap-level \
                     deprecated alias that warns)"
                )),
            },
            None => errors.push(format!(
                "  - {key}: launcher exports {exported}, but the flag has no env binding \
                 (the setting is silently ignored)"
            )),
        }
    }

    if !errors.is_empty() {
        panic!(
            "Launcher env names do not match the CLI ({launcher_path}):\n{}\n\n\
             To fix: update TOML_KEY_ENVVAR in the launcher (or remove the entry when the \
             key follows the INFLUXDB3_ + SCREAMING_SNAKE default) so it exports the env \
             name the clap flag binds, and update the `(env: ...)` comment in {config_path}.",
            errors.join("\n")
        );
    }
}

/// `(env: NAME)` hints in the packaged `.conf` template must name the
/// variable the launcher exports for the key that follows, i.e. the canonical
/// env name clap binds for that flag. A rename that only changes the env
/// name (flag long name unchanged) leaves the `.conf` key untouched, so
/// nothing about the template looks stale in review (#5465).
#[test]
fn test_config_file_env_comments_match_cli() {
    use crate::commands::serve::Config;
    use influxdb3_startup::env_compat::ENV_ALIASES;

    let config_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.circleci/packages/influxdb3/fs/usr/share/influxdb3/influxdb3-core.conf"
    );
    let config = std::fs::read_to_string(config_path)
        .unwrap_or_else(|e| panic!("Failed to read config file at {config_path}: {e}"));

    let mut cli_env = HashMap::new();
    extract_cli_env_names(&Config::command(), &mut cli_env);
    let legacy: HashMap<&str, &str> = ENV_ALIASES.iter().map(|(new, old)| (*old, *new)).collect();
    // Anchor on the closing paren only: hints appear both as `(env: NAME)`
    // and mid-parenthetical as `(...; env: NAME)`.
    let env_hint = regex::Regex::new(r"\benv: ([A-Z0-9_]+)\)").expect("valid regex");

    let mut errors = Vec::new();
    let mut pending: Option<(usize, String)> = None;
    for (idx, raw) in config.lines().enumerate() {
        let line = raw.trim();
        if let Some(cap) = env_hint.captures(line) {
            pending = Some((idx + 1, cap[1].to_string()));
            continue;
        }
        // `#` followed by whitespace is prose or an example inside a comment
        // block; a commented-out default key is `#key=value` with no gap.
        if line
            .strip_prefix('#')
            .is_some_and(|r| r.starts_with([' ', '\t']))
        {
            continue;
        }
        let Some(key) = extract_config_file_options(line).into_iter().next() else {
            continue;
        };
        let Some((hint_line, hinted)) = pending.take() else {
            errors.push(format!(
                "  - line {}: {key} has no (env: ...) hint",
                idx + 1
            ));
            continue;
        };
        let Some(bound) = cli_env.get(canonical_conf_key(&key)) else {
            // Covered by test_config_file_cli_option_drift.
            continue;
        };
        let expected = bound
            .as_deref()
            .map(|b| legacy.get(b).copied().unwrap_or(b));
        match expected {
            Some(expected) if expected == hinted => {}
            Some(expected) => errors.push(format!(
                "  - line {hint_line}: {key} hints (env: {hinted}) but the flag reads {expected}"
            )),
            None => errors.push(format!(
                "  - line {hint_line}: {key} hints (env: {hinted}) but the flag has no env binding"
            )),
        }
    }

    if !errors.is_empty() {
        panic!(
            "`(env: ...)` hints in {config_path} do not match the CLI:\n{}\n\n\
             To fix: name the env var clap binds for the flag (its canonical INFLUXDB3_ \
             spelling when the flag binds a legacy primary listed in ENV_ALIASES).",
            errors.join("\n")
        );
    }
}

/// Every `[env: NAME=]` in the hand-curated help text must be real: in
/// `serve.txt`/`serve_all.txt` each one sits on a continuation line of its
/// option row and must name the env var clap binds for that row's flag (its
/// canonical `ENV_ALIASES` spelling); in the other files it must be an env
/// name some clap arg binds. `--help` renders these files, not clap output,
/// so a rename that updates the clap `env` leaves the help text stale.
#[test]
fn help_text_env_names_are_bound() {
    use influxdb3_startup::env_compat::ENV_ALIASES;

    let mut bound = HashSet::new();
    extract_bound_env_names(&crate::Config::command(), &mut bound);
    let legacy: HashMap<&str, &str> = ENV_ALIASES.iter().map(|(new, old)| (*old, *new)).collect();
    // Help text must advertise the canonical INFLUXDB3_ spelling, so a bound
    // legacy primary is replaced, not kept: its own name has to fail below.
    for name in bound.clone() {
        if let Some(canonical) = legacy.get(name.as_str()) {
            bound.remove(&name);
            bound.insert(canonical.to_string());
        }
    }

    let help_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/help");
    let env_ref = regex::Regex::new(r"\[env: ([A-Z0-9_]+)=?\]").expect("valid regex");
    let mut errors = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(help_dir)
        .unwrap_or_else(|e| panic!("Failed to read {help_dir}: {e}"))
        .map(|entry| entry.expect("readable dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "txt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no help text files found in {help_dir}");
    let mut serve_env = HashMap::new();
    extract_cli_env_names(&crate::commands::serve::Config::command(), &mut serve_env);

    for path in files {
        let file = path.file_name().unwrap_or_default().to_string_lossy();
        // In the serve help files every `[env:]` sits on a continuation line
        // of its option row, so it is compared against that row's flag; the
        // other files get the tree-wide bound-name check.
        let per_row = file == "serve.txt" || file == "serve_all.txt";
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
        let mut row_flag: Option<String> = None;
        for (idx, line) in text.lines().enumerate() {
            // Option rows start at column 2 (`  --flag`, `  -v, --verbose`);
            // continuation lines are indented further.
            if line.strip_prefix("  ").is_some_and(|r| r.starts_with('-')) {
                row_flag = line
                    .split_whitespace()
                    .take(2)
                    .find_map(flag_name)
                    .map(str::to_string);
            }
            for cap in env_ref.captures_iter(line) {
                let name = &cap[1];
                // serve_env is keyed by primary long name; a row heading a
                // long that is not a serve arg falls back to the set check
                // (row parsability is test_help_all's concern).
                let row_env = per_row
                    .then_some(row_flag.as_ref())
                    .flatten()
                    .and_then(|flag| serve_env.get(flag.as_str()).map(|env| (flag, env)));
                if let Some((flag, env)) = row_env {
                    match env.as_deref().map(|e| legacy.get(e).copied().unwrap_or(e)) {
                        Some(expected) if name == expected => {}
                        Some(expected) => errors.push(format!(
                            "  - {file}:{}: --{flag} shows [env: {name}=] but the flag reads {expected}",
                            idx + 1
                        )),
                        None => errors.push(format!(
                            "  - {file}:{}: --{flag} shows [env: {name}=] but the flag has no env binding",
                            idx + 1
                        )),
                    }
                    continue;
                }
                if bound.contains(name) {
                    continue;
                }
                match legacy.get(name) {
                    Some(canonical) => errors.push(format!(
                        "  - {file}:{}: [env: {name}=] is a deprecated alias of {canonical}",
                        idx + 1
                    )),
                    None => errors.push(format!(
                        "  - {file}:{}: [env: {name}=] is not bound by any clap arg",
                        idx + 1
                    )),
                }
            }
        }
    }

    if !errors.is_empty() {
        panic!(
            "Help text names env vars the binary does not bind:\n{}\n\n\
             To fix: show the env name from the flag's clap `env` (its canonical INFLUXDB3_ \
             spelling when the flag binds a legacy primary listed in ENV_ALIASES).",
            errors.join("\n")
        );
    }
}

/// Visible `serve` options deliberately absent from `help/serve_all.txt`.
/// Every entry needs a reason; an option that should not be advertised at all
/// belongs on `hide = true` instead, which removes it from this check.
const HELP_ALL_EXCLUDED_OPTIONS: &[&str] = &[
    // Debug-only tokio console knobs, meant for in-house use. Not hidden at
    // the source because trogging is a shared crate pinned by influxdb_iox;
    // `hide = true` there would change IOx's help output on its next pin bump.
    "tokio-console-enabled",
    "tokio-console-client-buffer-capacity",
    "tokio-console-event-buffer-capacity",
    // Enterprise-only option that exists solely to emit a custom error in Core
    "cluster-id",
];

/// Long names (and aliases) of every argument, with hidden arguments dropped,
/// keyed by primary long name.
fn visible_arg_names(cmd: &clap::Command, out: &mut Vec<(String, Vec<String>)>) {
    for arg in cmd.get_arguments() {
        let id = arg.get_id().as_str();
        if id == "help" || id == "version" || id == "help-all" || arg.is_hide_set() {
            continue;
        }
        if let Some(long) = arg.get_long() {
            let mut names = vec![long.to_string()];
            if let Some(aliases) = arg.get_all_aliases() {
                names.extend(aliases.iter().map(|a| a.to_string()));
            }
            out.push((long.to_string(), names));
        }
    }
    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() != "help" {
            visible_arg_names(subcmd, out);
        }
    }
}

/// The `--long` name a help-text token starts with, if any.
fn flag_name(token: &str) -> Option<&str> {
    let rest = token.strip_prefix("--")?;
    let end = rest
        .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].trim_end_matches('-')).filter(|n| !n.is_empty())
}

/// `--long` names that head an option row (`  --flag ...`) in `help/serve_all.txt`.
/// Prose and examples may cite other commands' flags, so only rows are checked
/// for staleness.
fn help_text_option_rows(help: &str) -> HashSet<String> {
    help.lines()
        .filter_map(|line| {
            // A row may lead with a short flag: `-v, --verbose ...`.
            line.split_whitespace().take(2).find_map(flag_name)
        })
        .map(str::to_string)
        .collect()
}

/// `--help-all` renders the hand-maintained `help/serve_all.txt`, not clap
/// output, so an option is invisible there unless the PR adding it also edits
/// that file. Every visible option must appear (by long name or alias), and
/// every `--flag` the file mentions must still be a parsable argument.
#[test]
fn test_help_all_lists_visible_serve_options() {
    let help = include_str!("../../../help/serve_all.txt");
    let in_help = help_text_option_rows(help);

    let mut args = Vec::new();
    visible_arg_names(&Config::command(), &mut args);

    let mut known = HashSet::new();
    let mut missing = Vec::new();
    for (long, names) in &args {
        known.extend(names.iter().cloned());
        if !names.iter().any(|n| in_help.contains(n))
            && !HELP_ALL_EXCLUDED_OPTIONS.contains(&long.as_str())
        {
            missing.push(long.clone());
        }
    }
    missing.sort();

    let mut hidden_or_unknown = Vec::new();
    for arg in Config::command().get_arguments() {
        if let Some(long) = arg.get_long().filter(|_| arg.is_hide_set()) {
            known.insert(long.to_string());
            if let Some(aliases) = arg.get_all_aliases() {
                known.extend(aliases.iter().map(|a| a.to_string()));
            }
        }
    }
    let mut stale: Vec<_> = help_text_option_rows(help)
        .iter()
        .filter(|f| !known.contains(*f) && f.as_str() != "help" && f.as_str() != "help-all")
        .cloned()
        .collect();
    stale.sort();
    hidden_or_unknown.append(&mut stale);

    let mut error_msg = String::new();
    if !missing.is_empty() {
        error_msg.push_str("\n\nVisible serve options missing from help/serve_all.txt:\n");
        for opt in &missing {
            error_msg.push_str(&format!("  - --{opt}\n"));
        }
        error_msg.push_str(
            "\nTo fix: add an entry to help/serve_all.txt (see docs/cli-conventions.md), \
             mark the option `hide = true` if it must not be advertised, or list it in \
             HELP_ALL_EXCLUDED_OPTIONS with a reason.",
        );
    }
    if !hidden_or_unknown.is_empty() {
        error_msg.push_str("\n\nhelp/serve_all.txt option rows that are not serve arguments:\n");
        for opt in &hidden_or_unknown {
            error_msg.push_str(&format!("  - --{opt}\n"));
        }
        error_msg.push_str("\nTo fix: update or remove the stale entry.");
    }
    if !error_msg.is_empty() {
        panic!("help/serve_all.txt and serve CLI options are out of sync!{error_msg}");
    }
}
