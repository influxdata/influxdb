use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{thread, time};

use croaring::Bitmap;
use prost::Message;

static ITERATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct Foo {
    i: i32,
    v: Vec<u8>,
}

fn push(v: &mut Vec<u8>, i: u8) {
    v.push(i);
}

impl Foo {
    fn new(i: i32) -> Self {
        let mut v = Vec::with_capacity(1000);
        for i in 0..1000 {
            push(&mut v, i as u8);
        }
        Self { i, v }
    }

    fn foo(&self) {
        //  println!("{:?}", self.v.iter().fold(0u8, |a, b| a.wrapping_add(*b)));
    }
}

fn foo(fs: &[Foo]) {
    for f in fs {
        f.foo();
    }
}

fn work() {
    let mut rb1 = Bitmap::create();
    rb1.add(1);
    rb1.add(2);
    rb1.add(3);
    rb1.add(4);
    rb1.add(5);
    rb1.add(100);
    rb1.add(1000);
    rb1.run_optimize();

    for i in (2000..(100 * 1024 * 1024)).step_by(64 * 1024 + 1) {
        rb1.add(i);
    }
    rb1.run_optimize();

    assert!(rb1.contains(3));

    // loop {
    let v = vec![1, 2, 3, 4];
    let mut m: Vec<Foo> = v.into_iter().map(Foo::new).collect();

    foo(&m);
    //}
    m.remove(0);
    std::mem::forget(m);
}

fn worker() {
    loop {
        work();
        ITERATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

fn demo() {
    let heap_profiler_guard = heappy::HeapProfilerGuard::new(1);

    println!("start demo");

    for _ in 0..4 {
        thread::spawn(worker);
    }
    thread::sleep(time::Duration::from_secs(4));
    let iterations = ITERATIONS.fetch_add(0, Ordering::SeqCst);
    println!("Iterations: {}", iterations);

    let report = heap_profiler_guard.report();

    let filename = "/tmp/memflame.svg";
    println!("Writing to {}", filename);
    let mut file = std::fs::File::create(filename).unwrap();
    report.flamegraph(&mut file);

    let proto = report.pprof();

    let mut buf = vec![];
    proto.encode(&mut buf).unwrap();
    println!("proto size: {}", buf.len());
    let filename = "/tmp/memflame.pb";
    println!("Writing to {}", filename);
    let mut file = std::fs::File::create(filename).unwrap();
    file.write_all(&buf).unwrap();
}

fn main() {
    demo();
    println!("end demo");
}
