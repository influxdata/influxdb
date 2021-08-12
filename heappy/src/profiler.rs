use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use backtrace::Frame;
use prost::Message;
use spin::RwLock;

use crate::collector;

const MAX_DEPTH: usize = 32;

static HEAP_PROFILER_ENABLED: AtomicBool = AtomicBool::new(false);

lazy_static::lazy_static! {
    static ref HEAP_PROFILER_STATE: RwLock<ProfilerState<MAX_DEPTH>> = RwLock::new(Default::default());
}

/// RAII structure used to stop profiling when dropped. It is the only interface to access the heap profiler.
#[derive(Debug)]
pub struct HeapProfilerGuard {}

impl HeapProfilerGuard {
    pub fn new(period: usize) -> Self {
        Profiler::start(period);
        Self {}
    }

    pub fn report(self) -> HeapReport {
        std::mem::drop(self);
        HeapReport::new()
    }
}

impl Drop for HeapProfilerGuard {
    fn drop(&mut self) {
        Profiler::stop();
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Profiler;

impl Profiler {
    fn enabled() -> bool {
        HEAP_PROFILER_ENABLED.load(Ordering::SeqCst)
    }

    fn set_enabled(value: bool) {
        HEAP_PROFILER_ENABLED.store(value, Ordering::SeqCst)
    }

    fn start(period: usize) {
        let mut profiler = HEAP_PROFILER_STATE.write();
        *profiler = ProfilerState::new(period);
        std::mem::drop(profiler);

        Self::set_enabled(true);
    }

    fn stop() {
        Self::set_enabled(false);
    }

    // Called by malloc hooks to record a memory allocation event.
    pub(crate) unsafe fn track_allocated(size: isize) {
        thread_local!(static ENTERED: Cell<bool> = Cell::new(false));

        struct ResetOnDrop;

        impl Drop for ResetOnDrop {
            fn drop(&mut self) {
                ENTERED.with(|b| b.set(false));
            }
        }

        if !ENTERED.with(|b| b.replace(true)) {
            let _reset_on_drop = ResetOnDrop;
            if Self::enabled() {
                let mut profiler = HEAP_PROFILER_STATE.write();
                let mut sample_now = false;
                match size.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        profiler.allocated_objects += 1;
                        profiler.allocated_bytes += size;

                        if profiler.allocated_bytes >= profiler.next_sample {
                            profiler.next_sample =
                                profiler.allocated_bytes + profiler.period as isize;
                            sample_now = true;
                        }
                    }
                    #[cfg(not(feature = "measure_free"))]
                    std::cmp::Ordering::Less => {
                        // ignore
                    }
                    #[cfg(feature = "measure_free")]
                    std::cmp::Ordering::Less => {
                        profiler.freed_objects += 1;
                        profiler.freed_bytes += -size;

                        if profiler.freed_bytes >= profiler.next_free_sample {
                            profiler.next_free_sample =
                                profiler.freed_bytes + profiler.period as isize;
                            sample_now = true;
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        // ignore
                    }
                }

                if sample_now {
                    let mut bt = Frames::new();
                    // we're already holding a lock
                    backtrace::trace_unsynchronized(|frame| bt.push(frame));

                    profiler.collector.record(bt, size);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct HeapReport {
    data: HashMap<pprof::Frames, collector::MemProfileRecord>,
    period: usize,
}

impl HeapReport {
    fn new() -> Self {
        let mut profiler = HEAP_PROFILER_STATE.write();
        let collector = std::mem::take(&mut profiler.collector);

        let data = collector
            .into_iter()
            .map(|(frames, rec)| (frames.into(), rec))
            .collect();
        Self {
            data,
            period: profiler.period,
        }
    }

    /// flamegraph will write an svg flamegraph into writer.
    pub fn flamegraph<W>(&self, writer: W)
    where
        W: Write,
    {
        // the pprof crate already has all the necessary plumbing for the embedded flamegraph library, let's just render
        // the alloc_bytes stat with it.
        let data = self
            .data
            .iter()
            .map(|(frames, rec)| (frames.clone(), rec.alloc_bytes))
            .collect();

        let report = pprof::Report { data };

        let mut options: pprof::flamegraph::Options = Default::default();

        options.count_name = "bytes".to_string();
        options.colors =
            pprof::flamegraph::color::Palette::Basic(pprof::flamegraph::color::BasicPalette::Mem);

        report
            .flamegraph_with_options(writer, &mut options)
            .unwrap();
    }

    fn inner_pprof(&self) -> pprof::protos::Profile {
        use pprof::protos;
        let data = self.data.clone();

        let mut dudup_str = HashSet::new();
        for key in data.iter().map(|(key, _)| key) {
            for frame in &key.frames { 
                for symbol in frame {
                    dudup_str.insert(symbol.name());
                    dudup_str.insert(symbol.sys_name().into_owned());
                    dudup_str.insert(symbol.filename().into_owned());
                }
            }
        }
        // string table's first element must be an empty string
        let mut string_table = vec!["".to_owned()];
        string_table.extend(dudup_str.into_iter());

        let mut strings = HashMap::new();
        for (index, name) in string_table.iter().enumerate() {
            strings.insert(name.as_str(), index);
        }

        let mut samples = vec![];
        let mut loc_tbl = vec![];
        let mut fn_tbl = vec![];
        let mut functions = HashMap::new();
        for (key, rec) in &data { 
            let mut locs = vec![];
            for frame in &key.frames { 
                for symbol in frame {
                    let name = symbol.name();
                    if let Some(loc_idx) = functions.get(&name) {
                        locs.push(*loc_idx);
                        continue;
                    }
                    let sys_name = symbol.sys_name();
                    let filename = symbol.filename();
                    let lineno = symbol.lineno();
                    let function_id = fn_tbl.len() as u64 + 1;
                    let function = protos::Function {
                        id: function_id,
                        name: *strings.get(name.as_str()).unwrap() as i64,
                        system_name: *strings.get(sys_name.as_ref()).unwrap() as i64,
                        filename: *strings.get(filename.as_ref()).unwrap() as i64,
                        ..protos::Function::default()
                    };
                    functions.insert(name, function_id);
                    let line = protos::Line {
                        function_id,
                        line: lineno as i64,
                    };
                    let loc = protos::Location {
                        id: function_id,
                        line: vec![line],
                        ..protos::Location::default()
                    };
                    // the fn_tbl has the same length with loc_tbl
                    fn_tbl.push(function);
                    loc_tbl.push(loc);
                    // current frame locations
                    locs.push(function_id);
                }
            }
            let sample = protos::Sample {
                location_id: locs,
                #[cfg(feature = "measure_free")]
                value: vec![
                    rec.alloc_objects as i64,
                    rec.alloc_bytes as i64,
                    rec.in_use_objects() as i64,
                    rec.in_use_bytes() as i64,
                ],
                #[cfg(not(feature = "measure_free"))]
                value: vec![rec.alloc_objects as i64, rec.alloc_bytes as i64],
                ..protos::Sample::default()
            };
            samples.push(sample);
        }

        let mut push_string = |s: &str| {
            let idx = string_table.len();
            string_table.push(s.to_string());
            idx as i64
        };

        let alloc_objects_idx = push_string("alloc_objects");
        let count_idx = push_string("count");
        let alloc_space_idx = push_string("alloc_space");
        let bytes_idx = push_string("bytes");
        #[cfg(feature = "measure_free")]
        let inuse_objects_idx = push_string("inuse_objects");
        #[cfg(feature = "measure_free")]
        let inuse_space_idx = push_string("inuse_space");
        let space_idx = push_string("space");

        let sample_type = vec![
            protos::ValueType {
                r#type: alloc_objects_idx,
                unit: count_idx,
            },
            protos::ValueType {
                r#type: alloc_space_idx,
                unit: bytes_idx,
            },
            #[cfg(feature = "measure_free")]
            protos::ValueType {
                r#type: inuse_objects_idx,
                unit: count_idx,
            },
            #[cfg(feature = "measure_free")]
            protos::ValueType {
                r#type: inuse_space_idx,
                unit: bytes_idx,
            },
        ];

        let period_type = Some(pprof::protos::ValueType {
            r#type: space_idx,
            unit: bytes_idx,
        });

        protos::Profile {
            sample_type,
            default_sample_type: alloc_space_idx,
            sample: samples,
            string_table,
            period_type,
            period: self.period as i64,
            function: fn_tbl,
            location: loc_tbl,
            ..protos::Profile::default()
        }
    }

    /// produce a pprof proto (for use with go tool pprof and compatible visualizers)
    pub fn pprof(&self) -> pprof::protos::Profile {
        let mut proto = self.inner_pprof();

        let drop_frames_idx = proto.string_table.len();
        proto
            .string_table
            .push(".*::Profiler::track_allocated".to_string());
        proto.drop_frames = drop_frames_idx as i64;

        proto
    }

    pub fn write_pprof<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut buf = vec![];
        self.pprof().encode(&mut buf)?;
        writer.write_all(&buf)
    }
}

// Current profiler state, collection of sampled frames.
struct ProfilerState<const N: usize> {
    collector: collector::Collector<Frames<N>>,
    allocated_objects: isize,
    allocated_bytes: isize,
    #[cfg(feature = "measure_free")]
    freed_objects: isize,
    #[cfg(feature = "measure_free")]
    freed_bytes: isize,
    // take a sample when allocated crosses this threshold
    next_sample: isize,
    // take a sample when free crosses this threshold
    #[cfg(feature = "measure_free")]
    next_free_sample: isize,
    // take a sample every period bytes.
    period: usize,
}

impl<const N: usize> ProfilerState<N> {
    fn new(period: usize) -> Self {
        Self {
            collector: collector::Collector::new(),
            period,
            allocated_objects: 0,
            allocated_bytes: 0,
            #[cfg(feature = "measure_free")]
            freed_objects: 0,
            #[cfg(feature = "measure_free")]
            freed_bytes: 0,
            next_sample: period as isize,
            #[cfg(feature = "measure_free")]
            next_free_sample: period as isize,
        }
    }
}

impl<const N: usize> Default for ProfilerState<N> {
    fn default() -> Self {
        Self::new(1)
    }
}

struct Frames<const N: usize> {
    frames: [Frame; N],
    size: usize,
}

impl<const N: usize> Clone for Frames<N> {
    fn clone(&self) -> Self {
        let mut n = unsafe { Self::new() };
        for i in 0..self.size {
            n.frames[i] = self.frames[i].clone()
        }
        n.size = self.size;
        n
    }
}

impl<const N: usize> Frames<N> {
    #[allow(clippy::uninit_assumed_init)]
    unsafe fn new() -> Self {
        Self {
            frames: std::mem::MaybeUninit::uninit().assume_init(),
            size: 0,
        }
    }

    /// Push will push up to N frames in the frames array.
    unsafe fn push(&mut self, frame: &Frame) -> bool {
        self.frames[self.size] = frame.clone();
        self.size += 1;
        self.size < N
    }

    fn iter(&self) -> FramesIterator<N> {
        FramesIterator(self, 0)
    }
}

impl<const N: usize> Hash for Frames<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.iter()
            .for_each(|frame| frame.symbol_address().hash(state));
    }
}

impl<const N: usize> PartialEq for Frames<N> {
    fn eq(&self, other: &Self) -> bool {
        Iterator::zip(self.iter(), other.iter())
            .map(|(s1, s2)| s1.symbol_address() == s2.symbol_address())
            .all(|equal| equal)
    }
}

impl<const N: usize> Eq for Frames<N> {}

struct FramesIterator<'a, const N: usize>(&'a Frames<N>, usize);

impl<'a, const N: usize> Iterator for FramesIterator<'a, N> {
    type Item = &'a Frame;

    fn next(&mut self) -> Option<Self::Item> {
        if self.1 < self.0.size {
            let res = Some(&self.0.frames[self.1]);
            self.1 += 1;
            res
        } else {
            None
        }
    }
}

impl<const N: usize> From<Frames<N>> for pprof::Frames {
    fn from(bt: Frames<N>) -> Self {
        let frames = bt
            .iter()
            .map(|frame| {
                let mut symbols = Vec::new();
                backtrace::resolve_frame(frame, |symbol| {
                    if let Some(name) = symbol.name() {
                        let name = format!("{:#}", name);
                        if !name.starts_with("alloc::alloc::")
                            && name != "<alloc::alloc::Global as core::alloc::Allocator>::allocate"
                        {
                            symbols.push(symbol.into());
                        }
                    }
                });
                symbols
            })
            .collect();
        Self {
            frames,
            thread_name: "".to_string(),
            thread_id: 0,
        }
    }
}
