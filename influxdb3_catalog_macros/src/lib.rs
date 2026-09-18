//! Procedural macros for `influxdb3_catalog`.
//!
//! See [`catalog_record`] — the attribute applied to every catalog record type.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, Fields, ItemStruct, Meta, Token, Type, parse_macro_input};

mod shape;
mod types;

/// Declares a catalog record type.
///
/// Applied to a struct with named fields, this:
///
/// - derives `Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode,
///   bitcode::Decode` (add `#[derive(Copy)]` separately where the type is `Copy`);
/// - implements `CatalogRecord` — whose `RecordApply` half stays hand-written;
/// - implements the `Encode`/`Decode` bitcode bridge;
/// - registers the type with the `inventory` registry;
/// - checks the field types against the declared `shape` and against the
///   allowlist of types permitted on the wire.
///
/// ```ignore
/// #[catalog_record(id = record_ids::SET_STORAGE_MODE, shape = 0xbba17476)]
/// #[derive(Copy)]
/// pub struct SetStorageMode {
///     pub mode: StorageMode,
/// }
///
/// impl RecordApply for SetStorageMode {
///     fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> { ... }
///     fn event(&self) -> CatalogEvent { ... }
/// }
/// ```
///
/// # Arguments
///
/// - `id` (required): the `RecordId` constant for this record. Explicit
///   because it is the on-disk contract, and not every record's constant is
///   named after its type.
/// - `shape` (required): the field-shape fingerprint, `0x…`. A mismatch is a
///   compile error naming the expected value.
/// - `flags` (optional): defaults to `RecordFlags::none()`.
///
/// The record's `NAME` is always the type name.
///
/// Expands to paths rooted at `crate::`, so it is usable only within
/// `influxdb3_catalog`.
#[proc_macro_attribute]
pub fn catalog_record(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with Punctuated::<Meta, Token![,]>::parse_terminated);
    let item = parse_macro_input!(item as ItemStruct);

    match expand(args, item) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Parsed `#[catalog_record(..)]` arguments.
struct Args {
    id: Expr,
    shape: u32,
    shape_span: proc_macro2::Span,
    flags: Option<Expr>,
}

impl Args {
    fn parse(metas: Punctuated<Meta, Token![,]>) -> syn::Result<Self> {
        let mut id = None;
        let mut shape = None;
        let mut flags = None;

        for meta in metas {
            let Meta::NameValue(nv) = meta else {
                return Err(syn::Error::new(
                    meta.span(),
                    "expected `name = value`; supported arguments are `id`, `shape`, `flags`",
                ));
            };
            let key = nv
                .path
                .get_ident()
                .ok_or_else(|| syn::Error::new(nv.path.span(), "expected an argument name"))?
                .to_string();

            match key.as_str() {
                "id" => {
                    if id.replace(nv.value).is_some() {
                        return Err(syn::Error::new(nv.path.span(), "duplicate `id`"));
                    }
                }
                "shape" => {
                    let span = nv.value.span();
                    let Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Int(lit),
                        ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(
                            span,
                            "`shape` must be an integer literal, e.g. `shape = 0xbba17476`",
                        ));
                    };
                    if shape.replace(lit).is_some() {
                        return Err(syn::Error::new(nv.path.span(), "duplicate `shape`"));
                    }
                }
                "flags" => {
                    if flags.replace(nv.value).is_some() {
                        return Err(syn::Error::new(nv.path.span(), "duplicate `flags`"));
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        nv.path.span(),
                        format!(
                            "unknown argument `{other}`; supported arguments are `id`, `shape`, `flags`"
                        ),
                    ));
                }
            }
        }

        let id = id.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "missing `id = record_ids::SOME_CONST`",
            )
        })?;
        let shape = shape.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "missing `shape = 0x…` — build with a placeholder to be told the expected value",
            )
        })?;

        Ok(Self {
            id,
            shape: shape.base10_parse()?,
            shape_span: shape.span(),
            flags,
        })
    }
}

fn expand(metas: Punctuated<Meta, Token![,]>, item: ItemStruct) -> syn::Result<TokenStream2> {
    let args = Args::parse(metas)?;

    if !item.generics.params.is_empty() {
        return Err(syn::Error::new(
            item.generics.span(),
            "catalog records cannot be generic — the persisted bytes must be one fixed shape",
        ));
    }

    let Fields::Named(ref fields) = item.fields else {
        return Err(syn::Error::new(
            item.fields.span(),
            "catalog records must be structs with named fields",
        ));
    };

    let field_types: Vec<&Type> = fields.named.iter().map(|f| &f.ty).collect();
    for ty in &field_types {
        types::check(ty)?;
    }

    let actual = shape::fingerprint(&field_types);
    if actual != args.shape {
        return Err(syn::Error::new(
            args.shape_span,
            shape_mismatch_message(&item.ident, args.shape, actual),
        ));
    }

    let ident = &item.ident;
    let id = &args.id;
    let flags = args
        .flags
        .map(|f| quote!(#f))
        .unwrap_or_else(|| quote!(crate::format::RecordFlags::none()));

    Ok(quote! {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            serde::Serialize,
            bitcode::Encode,
            bitcode::Decode,
        )]
        #item

        impl crate::format::CatalogRecord for #ident {
            const ID: crate::format::RecordId = #id;
            const FLAGS: crate::format::RecordFlags = #flags;
            const NAME: &'static str = stringify!(#ident);
        }

        impl crate::format::Encode for #ident {
            fn encode(&self, buf: &mut ::std::vec::Vec<u8>) {
                buf.extend_from_slice(&bitcode::encode(self));
            }
        }

        impl crate::format::Decode for #ident {
            fn decode(buf: &[u8]) -> ::std::result::Result<Self, crate::format::FormatError> {
                bitcode::decode(buf).map_err(|_| crate::format::FormatError::InvalidRecordLength {
                    length: buf.len() as u32,
                })
            }
        }

        inventory::submit! {
            crate::format::RegisteredRecord::new::<#ident>()
        }
    })
}

fn shape_mismatch_message(ident: &syn::Ident, declared: u32, actual: u32) -> String {
    format!(
        "⚠ Field shape of catalog record ({ident}) changed ⚠\n\n\
         Declared shape:\n\n    {declared:#010x}\n\n\
         Actual shape:\n\n    {actual:#010x}\n\n\
         The fingerprint covers the field types, in order — adding, removing, reordering, \
         or retyping a field moves it. Field renames do not, because bitcode does not \
         encode field names.\n\n\
         Once a catalog record type has been shipped in a released version of the software, \
         it cannot be modified. To introduce new functionality, add a new record type.\n\n\
         If this is the first time you're adding this record type, or are making \
         modifications to it prior to releasing it, update the `shape` argument to the \
         actual value above.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse::Parser;

    fn parse(args: &str) -> syn::Result<Args> {
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse_str(args)?;
        Args::parse(metas)
    }

    #[test]
    fn flags_default_to_none() {
        let args = parse("id = record_ids::CREATE_ROLE, shape = 0x1").unwrap();
        assert_eq!(args.shape, 1);
        assert!(args.flags.is_none());
    }

    #[test]
    fn flags_are_passed_through() {
        let args = parse(
            "id = record_ids::SET_NODE_LICENSE_DIGEST, shape = 0xdeadbeef, \
             flags = RecordFlags::upgrade_safe()",
        )
        .unwrap();
        assert_eq!(args.shape, 0xdead_beef);
        assert!(args.flags.is_some());
    }

    fn err(args: &str) -> String {
        match parse(args) {
            Ok(_) => panic!("expected an error for `{args}`"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn rejects_bad_arguments() {
        assert!(err("shape = 0x1").contains("id"));
        assert!(err("id = record_ids::X").contains("shape"));
        assert!(err("id = record_ids::X, shape = 0x1, nme = \"X\"").contains("unknown argument"));
        assert!(err("id = record_ids::X, shape = \"0x1\"").contains("integer literal"));
        assert!(err("id = record_ids::X, id = record_ids::Y, shape = 0x1").contains("duplicate"));
    }
}
