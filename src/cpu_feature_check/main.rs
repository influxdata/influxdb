//! This program prints what available x86 features are available on this
//! processor

macro_rules! check_feature {
    ($name: tt) => {
        println!("  {:10}: {}", $name, std::is_x86_feature_detected!($name))
    };
}

fn main() {
    println!("Available CPU features on this machine:");

    // The list of possibilities was taken from
    // https://doc.rust-lang.org/reference/attributes/codegen.html#the-target_feature-attribute
    //
    // Features that are commented out are experimental
    check_feature!("aes");
    check_feature!("pclmulqdq");
    check_feature!("rdrand");
    check_feature!("rdseed");
    check_feature!("tsc");
    check_feature!("mmx");
    check_feature!("sse");
    check_feature!("sse2");
    check_feature!("sse3");
    check_feature!("ssse3");
    check_feature!("sse4.1");
    check_feature!("sse4.2");
    check_feature!("sse4a");
    check_feature!("sha");
    check_feature!("avx");
    check_feature!("avx2");
    check_feature!("avx512f");
    check_feature!("avx512cd");
    check_feature!("avx512er");
    check_feature!("avx512pf");
    check_feature!("avx512bw");
    check_feature!("avx512dq");
    check_feature!("avx512vl");
    //check_feature!("avx512ifma");
    // check_feature!("avx512vbmi");
    // check_feature!("avx512vpopcntdq");
    // check_feature!("avx512vbmi2");
    // check_feature!("avx512gfni");
    // check_feature!("avx512vaes");
    // check_feature!("avx512vpclmulqdq");
    // check_feature!("avx512vnni");
    // check_feature!("avx512bitalg");
    // check_feature!("avx512bf16");
    // check_feature!("avx512vp2intersect");
    //check_feature!("f16c");
    check_feature!("fma");
    check_feature!("bmi1");
    check_feature!("bmi2");
    check_feature!("abm");
    check_feature!("lzcnt");
    check_feature!("tbm");
    check_feature!("popcnt");
    check_feature!("fxsr");
    check_feature!("xsave");
    check_feature!("xsaveopt");
    check_feature!("xsaves");
    check_feature!("xsavec");
    //check_feature!("cmpxchg16b");
    check_feature!("adx");
    //check_feature!("rtm");
}
