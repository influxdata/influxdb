fn work() {
    let v = vec![65_u8, 66, 66, 10];
    println!("{:?}", &v);
}

fn demo() {
    // Using a period of 1 to catch all allocations.
    let heap_profiler_guard = heappy::HeapProfilerGuard::new(1);

    work();

    let report = heap_profiler_guard.report();

    let filename = "/tmp/memflame.svg";
    println!("Writing to {}", filename);
    let mut file = std::fs::File::create(filename).unwrap();
    report.flamegraph(&mut file);

    let filename = "/tmp/memflame.pb";
    println!("Writing to {}", filename);
    let mut file = std::fs::File::create(filename).unwrap();
    report.write_pprof(&mut file).unwrap();
}

fn main() {
    // cause some print before the demo or the memory profiler will show also the (expensive) lazy initialization of the print subsystem
    println!("starting demo");
    demo();
    println!("bye");
}
