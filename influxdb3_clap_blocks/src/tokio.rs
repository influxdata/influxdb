//! Config for the tokio main IO and DataFusion runtimes.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use paste::paste;

/// Tokio runtime type.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum TokioRuntimeType {
    /// Multi-thread runtime.
    #[default]
    MultiThread,

    /// New, alternative multi-thread runtime.
    ///
    /// Requires `tokio_unstable` compile-time flag.
    MultiThreadAlt,
}

#[cfg(unix)]
fn set_current_thread_priority(prio: i32) {
    // on linux setpriority sets the current thread's priority
    // (as opposed to the current process).
    unsafe { libc::setpriority(0, 0, prio) };
}

macro_rules! tokio_rt_config {
    (
        name = $name:ident ,
        num_threads_arg = $num_threads_arg:expr ,
        num_threads_env = $num_threads_env:expr ,
        num_threads_arg_alias = $num_threads_arg_alias:expr ,
        num_threads_env_alias = $num_threads_env_alias:expr ,
        default_thread_priority = $default_thread_priority:expr,
    ) => {
        paste! {
            #[doc = "CLI config for tokio " $name " runtime."]
            #[derive(Debug, Clone, clap::Parser)]
            #[allow(missing_copy_implementations)]
            pub struct [<Tokio $name:camel Config>] {
                #[doc = "Set the maximum number of " $name " runtime threads to use."]
                #[doc = ""]
                #[doc = "Defaults to the number of logical cores on the system."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_num_threads"),
                    long = $num_threads_arg,
                    alias = $num_threads_arg_alias,
                    env = $num_threads_env,
                    action
                )]
                pub num_threads: Option<NonZeroUsize>,

                #[doc = $name " tokio runtime type."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_type"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-type"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_TYPE"),
                    default_value_t = TokioRuntimeType::default(),
                    value_enum,
                    action
                )]
                pub runtime_type: TokioRuntimeType,

                #[doc = "Disable LIFO slot of " $name " runtime."]
                #[doc = ""]
                #[doc = "Requires `tokio_unstable` compile-time flag."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_disable_lifo"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-disable-lifo-slot"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_DISABLE_LIFO_SLOT"),
                    action
                )]
                pub disable_lifo: Option<bool>,

                #[doc = "Sets the number of scheduler ticks after which the scheduler of the " $name "tokio runtime"]
                #[doc = "will poll for external events (timers, I/O, and so on)."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_event_interval"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-event-interval"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_EVENT_INTERVAL"),
                    action
                )]
                pub event_interval: Option<NonZeroU32>,

                #[doc = "Sets the number of scheduler ticks after which the scheduler of the " $name " runtime"]
                #[doc = "will poll the global task queue."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_global_queue_interval"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-global-queue-interval"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_GLOBAL_QUEUE_INTERVAL"),
                    action
                )]
                pub global_queue_interval: Option<NonZeroU32>,

                #[doc = "Specifies the limit for additional threads spawned by the " $name " runtime."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_max_blocking_threads"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-max-blocking-threads"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_MAX_BLOCKING_THREADS"),
                    action
                )]
                pub max_blocking_threads: Option<NonZeroUsize>,

                #[doc = "Configures the max number of events to be processed per tick by the tokio " $name " runtime."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_max_io_events_per_tick"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-max-io-events-per-tick"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_MAX_IO_EVENTS_PER_TICK"),
                    action
                )]
                pub max_io_events_per_tick: Option<NonZeroUsize>,

                #[doc = "Sets a custom timeout for a thread in the blocking pool of the tokio " $name " runtime."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_thread_keep_alive"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-thread-keep-alive"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_THREAD_KEEP_ALIVE"),
                    value_parser = humantime::parse_duration
                )]
                pub thread_keep_alive: Option<Duration>,

                #[doc = "Set thread priority tokio " $name " runtime workers."]
                #[clap(
                    id = concat!(stringify!([<$name:lower>]), "_runtime_thread_priority"),
                    long = concat!(stringify!([<$name:lower>]), "-runtime-thread-priority"),
                    env = concat!("INFLUXDB3_", stringify!([<$name:upper>]), "_RUNTIME_THREAD_PRIORITY"),
                    default_value = $default_thread_priority,
                    action,
                )]
                pub thread_priority: Option<i32>,
            }

            impl [<Tokio $name:camel Config>] {
                pub fn copy_deprecated_env_aliases() {
                    if std::env::var($num_threads_env_alias).is_ok()
                            && std::env::var($num_threads_env).is_err()
                        {
                            eprintln!("WARN: Use of deprecated environment variable {}: replace with {}",  $num_threads_env_alias, $num_threads_env);
                            let v = std::env::var($num_threads_env_alias).unwrap();
                            unsafe { std::env::set_var($num_threads_env, v); }
                        }
                }

                /// Creates the tokio runtime builder.
                pub fn builder(&self) -> Result<::tokio::runtime::Builder, std::io::Error> {
                    self.builder_with_name(stringify!($name))
                }

                /// Creates the tokio runtime builder.
                pub fn builder_with_name(&self, name: &str) -> Result<::tokio::runtime::Builder, std::io::Error> {
                    // NOTE: no log macros will work here!
                    //
                    // That means use eprintln!() instead of error!() and so on. The log emitter
                    // requires a running tokio runtime and is initialised after this function.

                    let mut builder = match self.runtime_type {
                        TokioRuntimeType::MultiThread => tokio::runtime::Builder::new_multi_thread(),
                        TokioRuntimeType::MultiThreadAlt => {
                            #[cfg(tokio_unstable)]
                            {
                                tokio::runtime::Builder::new_multi_thread()
                            }
                            #[cfg(not(tokio_unstable))]
                            {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "multi-thread-alt runtime requires `tokio_unstable`",
                                ));
                            }
                        }
                    };

                    // enable subsystems
                    // - always enable timers
                    builder.enable_time();
                    builder.enable_io();

                    // set up proper thread names
                    let thread_counter = Arc::new(AtomicUsize::new(1));
                    let name = name.to_owned();
                    builder.thread_name_fn(move || {
                        format!("InfluxDB 3 Core Tokio {} {}", name, thread_counter.fetch_add(1, Ordering::SeqCst))
                    });

                    // worker thread count
                    let num_threads = match self.num_threads {
                        None => std::thread::available_parallelism()
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                        Some(n) => n,
                    };
                    builder.worker_threads(num_threads.get());

                    if self.disable_lifo == Some(true) {
                        #[cfg(tokio_unstable)]
                        {
                            builder.disable_lifo_slot();
                        }
                        #[cfg(not(tokio_unstable))]
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "disabling LIFO slot requires `tokio_unstable`",
                            ));
                        }
                    }

                    if let Some(x) = self.event_interval {
                        builder.event_interval(x.get());
                    }

                    if let Some(x) = self.global_queue_interval {
                        builder.global_queue_interval(x.get());
                    }

                    if let Some(x) = self.max_blocking_threads {
                        builder.max_blocking_threads(x.get());
                    }

                    if let Some(x) = self.max_io_events_per_tick {
                        builder.max_io_events_per_tick(x.get());
                    }

                    if let Some(x) = self.thread_keep_alive {
                        builder.thread_keep_alive(x);
                    }

                    #[allow(unused)]
                    if let Some(x) = self.thread_priority {
                        #[cfg(unix)]
                        {
                            builder.on_thread_start(move || set_current_thread_priority(x));
                        }
                        #[cfg(not(unix))]
                        {
                            use observability_deps::tracing::warn;

                            // use warning instead of hard error to allow for easier default settings
                            warn!("Setting worker thread priority not supported on this platform");
                        }
                    }

                    Ok(builder)
                }
            }
        }
    };
}

tokio_rt_config!(
    name = IO,
    num_threads_arg = "num-io-threads",
    num_threads_env = "INFLUXDB3_NUM_IO_THREADS",
    num_threads_arg_alias = "num-threads",
    num_threads_env_alias = "INFLUXDB3_NUM_THREADS",
    default_thread_priority = None,
);

tokio_rt_config!(
    name = Datafusion,
    num_threads_arg = "num-datafusion-threads",
    num_threads_env = "INFLUXDB3_NUM_DATAFUSION_THREADS",
    num_threads_arg_alias = "datafusion-num-threads",
    num_threads_env_alias = "INFLUXDB3_DATAFUSION_NUM_THREADS",
    default_thread_priority = "10",
);

#[cfg(test)]
mod tests;
