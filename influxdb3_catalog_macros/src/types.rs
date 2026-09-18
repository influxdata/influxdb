//! Allowlist for the types a catalog record may hold.
//!
//! Record bodies are the on-disk format, so a field may only name a type whose
//! encoding the catalog crate owns: a primitive, a `String`, a tuple, array,
//! `Option`, or `Vec` of those, or a bare identifier — which within the records
//! modules resolves to a wire type declared alongside the records.
//!
//! The rule that does the work is the rejection of qualified paths. It stops a
//! runtime type such as `influxdb3_authz::role::Permission` — whose layout the
//! catalog does not control — from being written into the log by reference.

use syn::spanned::Spanned;
use syn::{GenericArgument, PathArguments, Type};

const PRIMITIVES: &[&str] = &[
    "bool", "f32", "f64", "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32", "u64",
    "u128", "usize",
];

/// Containers that may wrap another allowed type.
const CONTAINERS: &[&str] = &["Option", "Vec"];

/// Check one field type against the allowlist.
pub(crate) fn check(ty: &Type) -> syn::Result<()> {
    match ty {
        Type::Array(array) => check(&array.elem),
        Type::Tuple(tuple) => tuple.elems.iter().try_for_each(check),
        Type::Path(path) if path.qself.is_none() => check_path(ty, &path.path),
        _ => Err(unsupported(ty)),
    }
}

fn check_path(ty: &Type, path: &syn::Path) -> syn::Result<()> {
    if path.leading_colon.is_some() || path.segments.len() > 1 {
        return Err(syn::Error::new(
            ty.span(),
            format!(
                "`{}` is a qualified path. A catalog record may only hold types whose encoding \
                 the catalog owns — declare a wire type in `records::types` and convert to the \
                 runtime type in `apply`.",
                render(ty)
            ),
        ));
    }

    let segment = &path.segments[0];
    let name = segment.ident.to_string();

    match &segment.arguments {
        PathArguments::None => Ok(()),
        // Every argument a const literal, e.g. `Reserved<16>`. Permitted where
        // a type parameter is not: the objection to a generic body is that it
        // has no single persisted shape, and a const argument does have one.
        // Each instantiation is also a distinct token sequence, so the shape
        // fingerprint tells `Reserved<16>` from `Reserved<32>` and a change of
        // width cannot slip through as compatible.
        //
        // Literals only. A named constant would fingerprint by name, so two
        // names for one width would disagree and renaming a constant would
        // move the shape without moving a byte.
        PathArguments::AngleBracketed(args) if !args.args.is_empty() => {
            if args.args.iter().all(is_const_literal) {
                return Ok(());
            }
            if !CONTAINERS.contains(&name.as_str()) {
                return Err(syn::Error::new(
                    ty.span(),
                    format!(
                        "`{name}` is generic. Only {} may take type parameters in a catalog \
                         record, and any other generic must be parameterised solely by const \
                         literals (e.g. `Reserved<16>`); a generic body has no single persisted \
                         shape.",
                        CONTAINERS.join(" and ")
                    ),
                ));
            }
            let inner: Vec<&Type> = args
                .args
                .iter()
                .filter_map(|arg| match arg {
                    GenericArgument::Type(t) => Some(t),
                    _ => None,
                })
                .collect();
            if inner.len() != args.args.len() || inner.len() != 1 {
                return Err(syn::Error::new(
                    ty.span(),
                    format!("`{name}` must have exactly one type parameter"),
                ));
            }
            check(inner[0])
        }
        PathArguments::AngleBracketed(_) | PathArguments::Parenthesized(_) => Err(unsupported(ty)),
    }
}

/// Whether a generic argument is a const *literal*, the only generic argument
/// permitted outside `Option`/`Vec`. See the call site for why.
fn is_const_literal(arg: &GenericArgument) -> bool {
    matches!(
        arg,
        GenericArgument::Const(syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(_),
            ..
        }))
    )
}

fn unsupported(ty: &Type) -> syn::Error {
    syn::Error::new(
        ty.span(),
        format!(
            "`{}` is not allowed in a catalog record. Fields may be primitives ({}), `String`, \
             arrays, tuples, `Option`/`Vec` of those, or a wire type declared in \
             `records::types`.",
            render(ty),
            PRIMITIVES.join(", "),
        ),
    )
}

fn render(ty: &Type) -> String {
    use quote::ToTokens;
    ty.to_token_stream().to_string().replace(' ', "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn allowed(ty: Type) -> bool {
        check(&ty).is_ok()
    }

    #[test]
    fn accepts_the_record_field_vocabulary() {
        assert!(allowed(parse_quote!(u64)));
        assert!(allowed(parse_quote!(bool)));
        assert!(allowed(parse_quote!(String)));
        assert!(allowed(parse_quote!(Option<String>)));
        assert!(allowed(parse_quote!(Vec<u64>)));
        assert!(allowed(parse_quote!(Vec<RolePermissionGrant>)));
        assert!(allowed(parse_quote!(Option<Vec<u8>>)));
        assert!(allowed(parse_quote!([u8; 16])));
        assert!(allowed(parse_quote!(StorageMode)));
        assert!(allowed(parse_quote!(Option<Vec<(String, String)>>)));
    }

    /// The allowlist reasons about tokens, not about whether a given width is
    /// meaningful for the type -- `Reserved`, for instance, rejects a
    /// zero-width tag at compile time itself.
    #[test]
    fn accepts_const_literal_generics() {
        assert!(allowed(parse_quote!(Reserved<16>)));
        assert!(allowed(parse_quote!(Spare<0>)));
        assert!(allowed(parse_quote!(Pad<4, 8>)));
    }

    /// The rationale for permitting const arguments is that each instantiation
    /// has one persisted shape. A type parameter does not, so it stays out.
    #[test]
    fn still_rejects_type_parameters_outside_the_containers() {
        let err = check(&parse_quote!(Reserved<String>)).unwrap_err();
        assert!(err.to_string().contains("is generic"), "{err}");
        assert!(check(&parse_quote!(Wrapper<u8>)).is_err());
        // Mixing a const with a type parameter reintroduces the problem.
        assert!(check(&parse_quote!(Reserved<16, String>)).is_err());
    }

    /// A named constant would fingerprint by name rather than value, so two
    /// names for one width would disagree and a rename would move the shape.
    #[test]
    fn rejects_non_literal_const_arguments() {
        assert!(check(&parse_quote!(Reserved<WIDTH>)).is_err());
        assert!(check(&parse_quote!(Reserved<{ WIDTH }>)).is_err());
    }

    #[test]
    fn rejects_qualified_paths() {
        let err = check(&parse_quote!(influxdb3_authz::role::Permission)).unwrap_err();
        assert!(err.to_string().contains("qualified path"), "{err}");
        assert!(check(&parse_quote!(::std::string::String)).is_err());
        assert!(check(&parse_quote!(std::time::Duration)).is_err());
    }

    #[test]
    fn rejects_unknown_generics() {
        let err = check(&parse_quote!(HashMap<String, u64>)).unwrap_err();
        assert!(err.to_string().contains("is generic"), "{err}");
        assert!(check(&parse_quote!(Arc<String>)).is_err());
    }

    #[test]
    fn rejects_references() {
        assert!(check(&parse_quote!(&'static str)).is_err());
        assert!(check(&parse_quote!(&[u8])).is_err());
    }

    #[test]
    fn checks_inside_tuples() {
        assert!(allowed(parse_quote!((u64, String))));
        assert!(check(&parse_quote!((u64, std::time::Duration))).is_err());
    }

    #[test]
    fn checks_inside_containers() {
        assert!(check(&parse_quote!(Vec<influxdb3_authz::role::Permission>)).is_err());
        assert!(check(&parse_quote!(Option<Arc<String>>)).is_err());
    }
}
