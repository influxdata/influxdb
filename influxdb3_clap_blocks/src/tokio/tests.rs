use super::*;

use clap::Parser;
use futures::FutureExt;
use std::{ffi::OsString, future::Future};
use tokio::net::TcpListener;

#[cfg(unix)]
#[test]
fn test_thread_priority() {
    assert_runtime_thread_property(
        TokioDatafusionConfig::parse_from(std::iter::empty::<OsString>())
            .builder()
            .unwrap(),
        || {
            assert_eq!(get_current_thread_priority(), 10);
        },
    );
}

#[test]
fn test_thread_name() {
    assert_runtime_thread_property(
        TokioIoConfig::parse_from(std::iter::empty::<OsString>())
            .builder()
            .unwrap(),
        || {
            assert_thread_name("InfluxDB 3 Core Tokio IO");
        },
    );
    assert_runtime_thread_property(
        TokioDatafusionConfig::parse_from(std::iter::empty::<OsString>())
            .builder()
            .unwrap(),
        || {
            assert_thread_name("InfluxDB 3 Core Tokio Datafusion");
        },
    );
    assert_runtime_thread_property(
        TokioDatafusionConfig::parse_from(std::iter::empty::<OsString>())
            .builder_with_name("foo")
            .unwrap(),
        || {
            assert_thread_name("InfluxDB 3 Core Tokio foo");
        },
    );
}

#[test]
fn test_io() {
    assert_runtime_thread_property_async(
        TokioIoConfig::parse_from(std::iter::empty::<OsString>())
            .builder()
            .unwrap(),
        || async move {
            assert!(is_io_enabled().await);
        },
    );
    assert_runtime_thread_property_async(
        TokioDatafusionConfig::parse_from(std::iter::empty::<OsString>())
            .builder()
            .unwrap(),
        || async move {
            assert!(is_io_enabled().await);
        },
    );
}

#[track_caller]
fn assert_runtime_thread_property<F>(builder: tokio::runtime::Builder, f: F)
where
    F: FnOnce() + Send + 'static,
{
    assert_runtime_thread_property_async(builder, || async move { f() });
}

#[track_caller]
fn assert_runtime_thread_property_async<F, Fut>(mut builder: tokio::runtime::Builder, f: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let rt = builder.build().unwrap();
    rt.block_on(async move {
        tokio::spawn(async move { f().await }).await.unwrap();
    });
}

#[cfg(unix)]
fn get_current_thread_priority() -> i32 {
    // on linux setpriority sets the current thread's priority
    // (as opposed to the current process).
    unsafe { libc::getpriority(0, 0) }
}

#[track_caller]
fn assert_thread_name(prefix: &'static str) {
    let thread = std::thread::current();
    let tname = thread.name().expect("thread is named");

    assert!(tname.starts_with(prefix), "Invalid thread name: {tname}",);
}

async fn is_io_enabled() -> bool {
    // the only way (I've found) to test if IO is enabled is to use it and observer if tokio panics
    TcpListener::bind("127.0.0.1:0")
        .catch_unwind()
        .await
        .is_ok()
}
