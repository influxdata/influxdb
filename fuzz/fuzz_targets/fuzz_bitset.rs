#![no_main]
use arbitrary::Unstructured;
use arrow_util::bitset::BitSet;
use libfuzzer_sys::fuzz_target;

#[derive(Debug, arbitrary::Arbitrary)]
struct Block {
    #[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=64))]
    size: usize,
    value: bool,
}

#[derive(Debug, arbitrary::Arbitrary)]
struct BoolVec(Vec<bool>);

impl From<Vec<Block>> for BoolVec {
    fn from(value: Vec<Block>) -> Self {
        BoolVec(
            value
                .into_iter()
                .flat_map(|x| vec![x.value; x.size])
                .collect(),
        )
    }
}

#[derive(Debug, arbitrary::Arbitrary)]
struct Ctx {
    bool_vec: BoolVec,
    block_vec: Vec<Block>,
}

fuzz_target!(|ctx: Ctx| {
    let Ctx {
        bool_vec,
        block_vec,
    } = ctx;

    // -- Fuzz append --
    let mut bitset = BitSet::new();
    for b in block_vec.iter() {
        if b.value {
            bitset.append_set(b.size);
        } else {
            bitset.append_unset(b.size);
        }
    }
    let from_block_vec: BoolVec = block_vec.into();
    for (i, &bit) in from_block_vec.0.iter().enumerate() {
        assert!(bit == bitset.get(i));
    }

    // -- Fuzz extend --
    let mut extended_bitset = BitSet::new();
    // Add basic data.
    for &bit in bool_vec.0.iter() {
        if bit {
            extended_bitset.append_set(1);
        } else {
            extended_bitset.append_unset(1);
        }
    }
    // Extend using the previously created bitset.
    extended_bitset.extend_from(&bitset);
    for (i, &bit) in bool_vec.0.iter().chain(from_block_vec.0.iter()).enumerate() {
        assert!(bit == extended_bitset.get(i));
    }
});
