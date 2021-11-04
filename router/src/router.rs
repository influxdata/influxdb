use data_types::router::Router as RouterConfig;

/// Router for a single database.
#[derive(Debug)]
pub struct Router {
    /// Router config.
    config: RouterConfig,
}

impl Router {
    /// Create new router from config.
    pub fn new(config: RouterConfig) -> Self {
        Self { config }
    }

    /// Router config.
    pub fn config(&self) -> &RouterConfig {
        &self.config
    }

    /// Router name.
    ///
    /// This is the same as the database that this router acts for.
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_getters() {
        let cfg = RouterConfig {
            name: String::from("my_router"),
            write_sharder: Default::default(),
            write_sinks: Default::default(),
            query_sinks: Default::default(),
        };
        let router = Router::new(cfg.clone());
        assert_eq!(router.config(), &cfg);
        assert_eq!(router.name(), "my_router");
    }
}
