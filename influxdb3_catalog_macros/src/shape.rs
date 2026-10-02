//! Field-shape fingerprinting.
//!
//! The fingerprint covers a record's field *types*, in order. Field names are
//! excluded because bitcode does not encode them, so a rename is free while an
//! added, removed, reordered, or retyped field moves the value.
//!
//! FNV-1a, not `DefaultHasher`: the value is written into source as
//! `shape = 0x…` and must be reproducible across compilers and toolchains.

use quote::ToTokens;
use syn::Type;

const FNV_OFFSET: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

/// Field separator, so `(A, BC)` and `(AB, C)` do not collide.
const SEPARATOR: u8 = 0x1f;

/// Fingerprint a record's field types, in declaration order.
pub(crate) fn fingerprint(field_types: &[&Type]) -> u32 {
    let mut hash = FNV_OFFSET;
    for ty in field_types {
        for byte in normalize(ty).bytes() {
            hash = round(hash, byte);
        }
        hash = round(hash, SEPARATOR);
    }
    hash
}

fn round(hash: u32, byte: u8) -> u32 {
    (hash ^ u32::from(byte)).wrapping_mul(FNV_PRIME)
}

/// Render a type as whitespace-free text, so that token spacing does not
/// affect the fingerprint.
fn normalize(ty: &Type) -> String {
    ty.to_token_stream()
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn shape(types: &[Type]) -> u32 {
        fingerprint(&types.iter().collect::<Vec<_>>())
    }

    #[test]
    fn spacing_does_not_matter() {
        let spaced: Type = syn::parse_str("Option < String >").unwrap();
        let tight: Type = parse_quote!(Option<String>);
        assert_eq!(shape(&[spaced]), shape(&[tight]));
    }

    #[test]
    fn field_order_matters() {
        let a: Type = parse_quote!(u64);
        let b: Type = parse_quote!(String);
        assert_ne!(shape(&[a.clone(), b.clone()]), shape(&[b, a]));
    }

    #[test]
    fn field_count_matters() {
        let ty: Type = parse_quote!(u64);
        assert_ne!(shape(std::slice::from_ref(&ty)), shape(&[ty.clone(), ty]));
    }

    #[test]
    fn separator_prevents_run_together_collisions() {
        let ab: Type = parse_quote!(Ab);
        let c: Type = parse_quote!(C);
        let a: Type = parse_quote!(A);
        let bc: Type = parse_quote!(Bc);
        assert_ne!(shape(&[ab, c]), shape(&[a, bc]));
    }

    #[test]
    fn retype_moves_the_fingerprint() {
        let signed: Type = parse_quote!(i64);
        let unsigned: Type = parse_quote!(u64);
        assert_ne!(shape(&[signed]), shape(&[unsigned]));
    }

    /// The fingerprint is written into source, so the algorithm is frozen.
    #[test]
    fn fingerprint_is_stable() {
        let empty: [Type; 0] = [];
        assert_eq!(shape(&empty), 0x811c_9dc5);

        let mode: Type = parse_quote!(StorageMode);
        assert_eq!(shape(&[mode]), 0xbba1_7476);

        let level: Type = parse_quote!(u8);
        let duration: Type = parse_quote!(u64);
        assert_eq!(shape(&[level, duration]), 0x8a45_7297);
    }

    /// The property that makes const-literal generics safe in a record field:
    /// a change of width is a change of shape, so it cannot pass as compatible.
    #[test]
    fn const_generic_widths_fingerprint_differently() {
        let narrow: Type = parse_quote!(Reserved<16>);
        let wide: Type = parse_quote!(Reserved<32>);
        assert_ne!(shape(&[narrow]), shape(&[wide]));
    }
}
