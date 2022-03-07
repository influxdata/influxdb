use rand::{distributions::Alphanumeric, thread_rng, Rng};

pub mod server_fixture;

/// Return a random string suitable for use as a database name
pub fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

// Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment variables are
// not set.
#[macro_export]
macro_rules! maybe_skip_integration {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

        let required_vars = ["TEST_INFLUXDB_IOX_CATALOG_DSN"];
        let unset_vars: Vec<_> = required_vars
            .iter()
            .filter_map(|&name| match env::var(name) {
                Ok(_) => None,
                Err(_) => Some(name),
            })
            .collect();
        let unset_var_names = unset_vars.join(", ");

        let force = env::var("TEST_INTEGRATION");

        if force.is_ok() && !unset_var_names.is_empty() {
            panic!(
                "TEST_INTEGRATION is set, \
                        but variable(s) {} need to be set",
                unset_var_names
            );
        } else if force.is_err() {
            eprintln!(
                "skipping end-to-end integration test - set {}TEST_INTEGRATION to run",
                if unset_var_names.is_empty() {
                    String::new()
                } else {
                    format!("{} and ", unset_var_names)
                }
            );

            return;
        } else {
            env::var("TEST_INFLUXDB_IOX_CATALOG_DSN")
                .expect("already checked TEST_INFLUXDB_IOX_CATALOG_DSN")
        }
    }};
}
