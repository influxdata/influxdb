use datafusion::config::ConfigExtension;

/// IOx-specific config extension prefix.
pub const IOX_CONFIG_PREFIX: &str = "iox";

macro_rules! cfg {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {
        $(#[doc = $struct_d])*
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl ::datafusion::config::ExtensionOptions for $struct_name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn ::datafusion::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> ::datafusion::error::Result<()> {
                match key {
                    $(
                       stringify!($field_name) => {
                        self.$field_name = value.parse().map_err(|e| {
                            ::datafusion::error::DataFusionError::Context(
                                format!(concat!("Error parsing {} as ", stringify!($t),), value),
                                Box::new(::datafusion::error::DataFusionError::External(Box::new(e))),
                            )
                        })?;
                        Ok(())
                       }
                    )*
                    _ => Err(::datafusion::error::DataFusionError::Internal(
                        format!(concat!("Config value \"{}\" not found on ", stringify!($struct_name)), key)
                    ))
                }
            }

            fn entries(&self) -> Vec<::datafusion::config::ConfigEntry> {
                vec![
                    $(
                        ::datafusion::config::ConfigEntry {
                            key: stringify!($field_name).to_owned(),
                            value: (self.$field_name != $default).then(|| self.$field_name.to_string()),
                            description: concat!($($d),*).trim(),
                        },
                    )*
                ]
            }
        }
    }
}

cfg! {
    /// Config options for IOx.
    pub struct IoxConfigExt {
        /// When splitting de-duplicate operations based on IOx partitions[^iox_part], this is the maximum number of IOx
        /// partitions that should be considered. If there are more partitions, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        ///
        ///
        /// [^iox_part]: "IOx partition" refers to a partition within the IOx catalog, i.e. a partition within the
        ///              primary key space. This is NOT the same as a DataFusion partition which refers to a stream
        ///              within the physical plan data flow.
        pub max_dedup_partition_split: usize, default = 100

        /// When splitting de-duplicate operations based on time-based overlaps, this is the maximum number of groups
        /// that should be considered. If there are more groups, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        pub max_dedup_time_split: usize, default = 100
    }
}

impl ConfigExtension for IoxConfigExt {
    const PREFIX: &'static str = IOX_CONFIG_PREFIX;
}
