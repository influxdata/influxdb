#![recursion_limit = "512"]
/// Prints what CPU features are used by the compiler by default.
///
/// Script from:
/// - <https://stackoverflow.com/questions/65156743/what-target-features-uses-rustc-by-default>
/// - <https://gist.github.com/AngelicosPhosphoros/4f8c9f08656e0812f4ed3560e53bd600>

// This script prints all cpu features which active in this build.
// There are 3 steps in usage of script:
// 1. get list of features using `rustc --print target-features`
// 2. put it into script (it has values actual for 2020-12-06 for x86-64 target).
// 3. run script.

fn pad_name(s: &str) -> String {
    let mut res = s.to_string();
    while res.len() < 30 {
        res.push(' ');
    }
    res
}

macro_rules! print_if_feature_enabled {
    () => {};
    ($feature:literal $(, $feats:literal)*)=>{
        if cfg!(target_feature = $feature){
            println!("feature {}", pad_name($feature));
        }
        print_if_feature_enabled!($($feats),*)
    }
}

fn main() {
    println!("rustc is using the following target options");

    print_if_feature_enabled!(
        "16bit-mode",
        "32bit-mode",
        "3dnow",
        "3dnowa",
        "64bit",
        "64bit-mode",
        "adx",
        "aes",
        "amx-bf16",
        "amx-int8",
        "amx-tile",
        "avx",
        "avx2",
        "avx512bf16",
        "avx512bitalg",
        "avx512bw",
        "avx512cd",
        "avx512dq",
        "avx512er",
        "avx512f",
        "avx512ifma",
        "avx512pf",
        "avx512vbmi",
        "avx512vbmi2",
        "avx512vl",
        "avx512vnni",
        "avx512vp2intersect",
        "avx512vpopcntdq",
        "bmi",
        "bmi2",
        "branchfusion",
        "cldemote",
        "clflushopt",
        "clwb",
        "clzero",
        "cmov",
        "cx16",
        "cx8",
        "enqcmd",
        "ermsb",
        "f16c",
        "false-deps-lzcnt-tzcnt",
        "false-deps-popcnt",
        "fast-11bytenop",
        "fast-15bytenop",
        "fast-7bytenop",
        "fast-bextr",
        "fast-gather",
        "fast-hops",
        "fast-lzcnt",
        "fast-scalar-fsqrt",
        "fast-scalar-shift-masks",
        "fast-shld-rotate",
        "fast-variable-shuffle",
        "fast-vector-fsqrt",
        "fast-vector-shift-masks",
        "fma",
        "fma4",
        "fsgsbase",
        "fxsr",
        "gfni",
        "idivl-to-divb",
        "idivq-to-divl",
        "invpcid",
        "lea-sp",
        "lea-uses-ag",
        "lvi-cfi",
        "lvi-load-hardening",
        "lwp",
        "lzcnt",
        "macrofusion",
        "merge-to-threeway-branch",
        "mmx",
        "movbe",
        "movdir64b",
        "movdiri",
        "mpx",
        "mwaitx",
        "nopl",
        "pad-short-functions",
        "pclmul",
        "pconfig",
        "pku",
        "popcnt",
        "prefer-128-bit",
        "prefer-256-bit",
        "prefer-mask-registers",
        "prefetchwt1",
        "prfchw",
        "ptwrite",
        "rdpid",
        "rdrnd",
        "rdseed",
        "retpoline",
        "retpoline-external-thunk",
        "retpoline-indirect-branches",
        "retpoline-indirect-calls",
        "rtm",
        "sahf",
        "serialize",
        "seses",
        "sgx",
        "sha",
        "shstk",
        "slow-3ops-lea",
        "slow-incdec",
        "slow-lea",
        "slow-pmaddwd",
        "slow-pmulld",
        "slow-shld",
        "slow-two-mem-ops",
        "slow-unaligned-mem-16",
        "slow-unaligned-mem-32",
        "soft-float",
        "sse",
        "sse-unaligned-mem",
        "sse2",
        "sse3",
        "sse4.1",
        "sse4.2",
        "sse4a",
        "ssse3",
        "tbm",
        "tsxldtrk",
        "use-aa",
        "use-glm-div-sqrt-costs",
        "vaes",
        "vpclmulqdq",
        "vzeroupper",
        "waitpkg",
        "wbnoinvd",
        "x87",
        "xop",
        "xsave",
        "xsavec",
        "xsaveopt",
        "xsaves"
    );
}
