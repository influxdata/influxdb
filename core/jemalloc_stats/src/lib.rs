#![expect(missing_copy_implementations)]

use std::{sync::OnceLock, time::Duration};

use tikv_jemalloc_ctl::{epoch as epoch_ctl, stats};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::io::ReaderStream;

mod monitor;

pub use monitor::{AllocationMonitor, AllocationMonitorError};

/// Baked-in default `malloc_conf` shared by each binary in the workspace.
///
/// Callers should invoke [`install_default_malloc_conf_for_profiling!`] rather than
/// wiring up the `#[unsafe(no_mangle)] pub static malloc_conf` declarations by hand;
/// the macro handles the platform-conditional symbol naming (`malloc_conf`
/// vs `_rjem_malloc_conf`) and length inference. The single source
/// is this constant — change the bytes here and every binary picks it up.
///
/// The type is a fixed-size array to match the jemalloc C declaration
/// `const char *malloc_conf`. See `docs/heap-profiling.md`
/// for the flag breakdown.
pub const DEFAULT_MALLOC_CONF: &[u8; 45] = b"prof:true,prof_active:true,lg_prof_sample:20\0";

/// Install the baked-in [`DEFAULT_MALLOC_CONF`] as the binary's `malloc_conf`
/// symbol, replacing jemalloc's weak default at link time.
///
/// Wraps the cfg-gated dual-symbol-name (`malloc_conf` on
/// Linux glibc / *BSD / illumos, `_rjem_malloc_conf` on the
/// `NO_UNPREFIXED_MALLOC_TARGETS` platforms — macOS / iOS / Android /
/// Dragonfly / musl) so each call site is one line. The static's array
/// length is inferred from [`DEFAULT_MALLOC_CONF`].
#[macro_export]
macro_rules! install_default_malloc_conf_for_profiling {
    () => {
        // Unprefixed symbol: Linux glibc, FreeBSD, NetBSD, OpenBSD, illumos.
        // Windows linkers don't support overriding `malloc_conf`, so no symbol is exported
        #[cfg(not(any(
            target_os = "windows",
            target_os = "macos",
            target_os = "ios",
            target_os = "android",
            target_os = "dragonfly",
            target_env = "musl",
        )))]
        #[allow(non_upper_case_globals)]
        #[unsafe(no_mangle)]
        pub static malloc_conf: &'static [u8; $crate::DEFAULT_MALLOC_CONF.len()] =
            $crate::DEFAULT_MALLOC_CONF;

        // Prefixed symbol: macOS, iOS, Android, Dragonfly, musl.
        #[cfg(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "android",
            target_os = "dragonfly",
            target_env = "musl",
        ))]
        #[allow(non_upper_case_globals)]
        #[unsafe(export_name = "_rjem_malloc_conf")]
        pub static malloc_conf: &'static [u8; $crate::DEFAULT_MALLOC_CONF.len()] =
            $crate::DEFAULT_MALLOC_CONF;
    };
}

/// Streaming reader for a jemalloc heap profile produced by
/// [`dump_heap_profile`]. Yields `Result<Bytes, io::Error>` chunks.
pub type HeapProfileStream = ReaderStream<tokio::fs::File>;

/// Name of the env var operators set to override the baked-in `malloc_conf`
/// at process start, resolved for the current target.
///
/// `tikv-jemalloc-sys` prefixes the `MALLOC_CONF` env name on the platforms
/// in its `NO_UNPREFIXED_MALLOC_TARGETS` list, so the right name to suggest
/// in a user-facing message differs by build target.
pub const MALLOC_CONF_ENV: &str = if cfg!(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    target_os = "dragonfly",
    target_env = "musl",
)) {
    "_RJEM_MALLOC_CONF"
} else {
    "MALLOC_CONF"
};

/// Errors returned by [`dump_heap_profile`].
#[derive(Debug, thiserror::Error)]
pub enum HeapDumpError {
    #[error("tempfile path was not valid C string (non-utf8 or interior NUL)")]
    BadTempPath,
    #[error("heap profiling is disabled; restart with {MALLOC_CONF_ENV}=prof:true to enable")]
    ProfilingDisabled,
    #[error("jemalloc prof.dump failed: {0}")]
    Jemalloc(#[from] tikv_jemalloc_ctl::Error),
    #[error("tempfile i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("blocking task join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Trigger a jemalloc heap profile dump and return a streaming reader for
/// the resulting `heap_v2` bytes.
///
/// Requires jemalloc built with `--enable-prof` (i.e. `tikv-jemallocator`
/// `profiling` feature) AND the binary started with `prof:true` in the
/// platform-appropriate `MALLOC_CONF` env var (or via a baked-in
/// `malloc_conf` static — see [`DEFAULT_MALLOC_CONF`]). When profiling is
/// disabled at startup this returns [`HeapDumpError::ProfilingDisabled`].
///
/// The function handles all file machinery internally: it allocates a
/// tempfile, runs jemalloc's synchronous `prof.dump`, unlinks the path, and
/// hands back an open fd wrapped in a [`HeapProfileStream`]. The tempfile
/// is unlinked before this call returns — POSIX semantics keep the inode
/// alive via the open fd, so no profile bytes remain on disk after the
/// stream is dropped.
pub async fn dump_heap_profile() -> Result<HeapProfileStream, HeapDumpError> {
    let std_file = tokio::task::spawn_blocking(dump_heap_profile_blocking).await??;
    Ok(ReaderStream::new(tokio::fs::File::from_std(std_file)))
}

/// Blocking core of [`dump_heap_profile`]. Always invoked under
/// `spawn_blocking`; separated for testability.
fn dump_heap_profile_blocking() -> Result<std::fs::File, HeapDumpError> {
    use std::ffi::CString;
    use std::os::raw::c_char;

    // Pre-check: when prof:false at startup, the prof.dump mallctl is not
    // registered and jemalloc returns an unhelpful EINVAL. opt.prof reflects
    // the effective startup value and is always readable when tikv-jemallocator
    // uses the profiling feature.
    //
    // SAFETY: opt.prof is a jemalloc startup option declared as bool in the
    // mallctl ABI; concurrent reads are safe, and a 1-byte C bool maps
    // cleanly to Rust bool.
    let prof_enabled: bool =
        unsafe { tikv_jemalloc_ctl::raw::read(b"opt.prof\0") }.unwrap_or(false);
    if !prof_enabled {
        return Err(HeapDumpError::ProfilingDisabled);
    }

    let tmp = tempfile::Builder::new()
        .prefix("jeprof.")
        .suffix(".heap")
        .tempfile()?;
    let cstr = tmp
        .path()
        .to_str()
        .and_then(|s| CString::new(s).ok())
        .ok_or(HeapDumpError::BadTempPath)?;

    // SAFETY: prof.dump takes a NUL-terminated `*const c_char` that jemalloc
    // copies before returning, so cstr only needs to outlive the call.
    // jemalloc serializes concurrent invocations via its internal
    // prof_dump_mtx, so this is safe to call from multiple threads.
    unsafe { tikv_jemalloc_ctl::raw::write::<*const c_char>(b"prof.dump\0", cstr.as_ptr()) }?;

    let (file, path) = tmp.into_parts();
    drop(path);
    Ok(file)
}

/// A singleton handle to the jemalloc [`Refresher`] task.
///
/// Callers interested in obtaining jemalloc statistics should call
/// [`Refresher::handle()`] to obtain periodic updates.
///
/// The first reference to [`STATS`] MUST be made from within an async tokio
/// runtime because a background tokio task is spawned by the initialised
/// [`Refresher`].
pub static STATS: OnceLock<Refresher> = OnceLock::new();

/// Defines the frequency at which updated [`Stats`] are obtained and published.
///
/// Obtaining refreshed statistics from jemalloc is not free, so tuning this to
/// a short duration is discouraged due to additional overhead.
///
/// This value is intentionally not a round multiple of some scrape interval to
/// avoid sample aliasing.
pub const REFRESH_INTERVAL: Duration = Duration::from_millis(9142);

/// A snapshot of Jemalloc's internal allocation statistics.
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub active: usize,
    pub allocated: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub resident: usize,
    pub retained: usize,
}

/// A handle to the singleton statistic refresh task.
///
/// Callers can obtain the latest [`Stats`] and change notifications from the
/// [`Refresher::handle()`].
///
/// Note that the inital [`Stats`] values before the first refresh is always 0.
///
/// Dropping this type stops updating the published [`Stats`].
#[derive(Debug)]
pub struct Refresher {
    rx: watch::Receiver<Stats>,
    refresh_task: JoinHandle<()>,
}

impl Refresher {
    /// Construct a new [`Stats`].
    ///
    /// Intentionally non-pub to enforce a singleton exposed via [`STATS`].
    pub fn new(tick_duration: Duration) -> Self {
        let (tx, rx) = watch::channel(Stats::default());

        Self {
            rx,

            // Spawn a background task to ask jemalloc to refresh the statistics
            // periodically, and publish the result.
            refresh_task: tokio::task::spawn(refresh(tx, tick_duration)),
        }
    }

    /// Obtain a [`watch::Receiver`] that receives the latest [`Stats`] as they
    /// are published.
    ///
    /// Statistics are refreshed every [`REFRESH_INTERVAL`].
    ///
    /// Callers SHOULD clone the [`Stats`] to reference them, rather than
    /// holding a long-lived reference to prevent blocking the publisher. See
    /// the [`watch::Receiver`] docs.
    ///
    /// ```rust
    /// # fn do_slow_thing() {}
    /// # let _guard = tokio::runtime::Runtime::new().unwrap().enter();
    /// # let REFRESH_INTERVAL = std::time::Duration::from_millis(9100);
    /// let handle = jemalloc_stats::STATS.get_or_init(|| jemalloc_stats::Refresher::new(REFRESH_INTERVAL)).handle();
    ///
    /// // Good:
    /// let stats = handle.borrow().clone();
    ///
    /// // Bad:
    /// let stats = handle.borrow();
    /// do_slow_thing();
    /// drop(stats);
    /// ```
    pub fn handle(&self) -> watch::Receiver<Stats> {
        self.rx.clone()
    }
}

impl Drop for Refresher {
    fn drop(&mut self) {
        self.refresh_task.abort()
    }
}

async fn refresh(tx: watch::Sender<Stats>, tick_duration: Duration) {
    let epoch = epoch_ctl::mib().unwrap();
    let active = stats::active::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    let metadata = stats::metadata::mib().unwrap();
    let mapped = stats::mapped::mib().unwrap();
    let resident = stats::resident::mib().unwrap();
    let retained = stats::retained::mib().unwrap();

    loop {
        // Have jemalloc refresh the internal statistics.
        epoch.advance().unwrap();

        // Read those statistics.
        let s = Stats {
            active: active.read().unwrap(),
            allocated: allocated.read().unwrap(),
            metadata: metadata.read().unwrap(),
            mapped: mapped.read().unwrap(),
            resident: resident.read().unwrap(),
            retained: retained.read().unwrap(),
        };

        // Make them available to all consumers.
        if tx.send(s).is_err() {
            // The Refresher type always retains a receiver handle, so if there
            // are no receivers, the Refresher has dropped and this task should
            // stop.
            return;
        }

        tokio::time::sleep(tick_duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert that a non-zero amount of data is allocated and eventually
    /// reported.
    #[tokio::test]
    async fn test_stats() {
        let stats = STATS.get_or_init(|| Refresher::new(REFRESH_INTERVAL));
        let handle = stats.handle();

        tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                if handle.borrow().active > 0 {
                    // Success!
                    return;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("timeout waiting to see non-zero allocator stats");
    }
}
