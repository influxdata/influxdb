//! Guards catalog wire enums against bitcode packing-bucket changes.
//!
//! bitcode's derive encodes an enum's variant tag by packing it against the
//! type's *variant count*, and never writes that choice into the stream. A
//! reader and a writer agree on the bytes only when their variant counts fall
//! in the same packing bucket, whose ceilings are 2, 3, 4, 6, 16 and 256.
//! Adding a variant that crosses a ceiling makes catalogs written by earlier
//! releases fail to decode, and can decode them to different values without
//! reporting an error. See issue #4905.
//!
//! Golden-byte assertions cannot cover this. `assert_encoding_stable!` encodes
//! one value, and a single variant tag packs to the same byte under every
//! bucket, so those assertions hold across a crossing. A record-level
//! `assert_roundtrip!` fixture only detects one if it happens to hold enough
//! elements of the enum: three for the 6 -> 7 crossing, six for 2 -> 3.
//!
//! The reference is the newest release tag older than this crate's version, so
//! it follows the version bump and needs no upkeep. On a release branch the
//! crate version selects that line's newest tag, which is the right baseline
//! there. Nothing is checked in for a developer to edit or refresh.
//!
//! The test reports that it did not run, rather than failing, when git or the
//! tags are unavailable — a shallow clone, a source tarball, a fresh fork.
//! That is suppressed when `CI` is set, so it cannot silently skip in CI.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// bitcode packing
// ---------------------------------------------------------------------------

/// Ceiling of the packing bucket bitcode selects for `variant_count`.
///
/// Mirrors `Packing::new(N - 1)` in bitcode 0.6.9 (`src/pack.rs`), which
/// `pack_bytes_less_than::<N>` uses to choose the packing for variant tags.
fn packing_bucket(variant_count: usize) -> usize {
    match variant_count.saturating_sub(1) {
        0 | 1 => 2,
        2 => 3,
        3 => 4,
        4 | 5 => 6,
        6..=15 => 16,
        _ => 256,
    }
}

// ---------------------------------------------------------------------------
// Reading the enums
// ---------------------------------------------------------------------------

/// Is `path` a non-test source file of the record format?
///
/// Matches both repository layouts: `oss/influxdb3_catalog/src/format/...`
/// here, and `influxdb3_catalog/src/format/...` in the synced oss repo.
fn is_format_source(path: &str) -> bool {
    path.ends_with(".rs")
        && path.contains("influxdb3_catalog/src/")
        && path.contains("/format/")
        && !path.contains("/tests/")
        && !path.ends_with("tests.rs")
}

/// Does this item derive bitcode's `Encode`, in any of the spellings used
/// across the format modules (`Encode`, `bitcode::Encode`)?
fn derives_bitcode_encode(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("derive") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|derived| {
            if derived
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "Encode")
            {
                found = true;
            }
            Ok(())
        });
        found
    })
}

fn has_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| match &attr.meta {
        syn::Meta::List(list) if list.path.is_ident("cfg") => {
            list.tokens.to_string().contains("test")
        }
        _ => false,
    })
}

fn collect_enums(items: &[syn::Item], path: &str, out: &mut BTreeMap<String, usize>) {
    for item in items {
        match item {
            syn::Item::Enum(item) if derives_bitcode_encode(&item.attrs) => {
                let name = item.ident.to_string();
                let count = item.variants.len();
                if let Some(previous) = out.insert(name.clone(), count) {
                    assert_eq!(
                        previous, count,
                        "two enums named {name} with different variant counts ({path})"
                    );
                }
            }
            syn::Item::Mod(module) if !has_cfg_test(&module.attrs) => {
                if let Some((_, items)) = &module.content {
                    collect_enums(items, path, out);
                }
            }
            _ => {}
        }
    }
}

/// Variant counts of every enum in `sources` that derives bitcode's `Encode`.
fn enum_variant_counts(sources: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (path, text) in sources {
        let file =
            syn::parse_file(text).unwrap_or_else(|e| panic!("could not parse {path} as Rust: {e}"));
        collect_enums(&file.items, path, &mut counts);
    }
    counts
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Format sources in the working tree.
fn worktree_sources() -> Option<Vec<(String, String)>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    if !root.is_dir() {
        return None;
    }
    let mut sources = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            // Normalise so `is_format_source` sees the same shape as git paths.
            let display = path.to_string_lossy().replace('\\', "/");
            let Some(idx) = display.find("influxdb3_catalog/src/") else {
                continue;
            };
            if is_format_source(&display[idx..]) {
                sources.push((
                    display[idx..].to_string(),
                    std::fs::read_to_string(&path).ok()?,
                ));
            }
        }
    }
    sources.sort();
    (!sources.is_empty()).then_some(sources)
}

/// Run git in the crate directory, reporting the command and its stderr on
/// failure. Opaque git errors are hard to act on from a CI log, so every path
/// out of here carries what actually went wrong.
fn git(args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .map_err(|e| format!("could not run `git {}`: {e}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "`git {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Format sources as they were at `tag`.
fn tag_sources(tag: &str) -> Result<Vec<(String, String)>, String> {
    // `--full-tree` so paths are reported from the repository root rather than
    // the crate directory this runs in.
    let listing = git(&["ls-tree", "--full-tree", "-r", "--name-only", tag])?;
    let listed = listing.lines().count();
    let mut sources = Vec::new();
    for path in listing.lines().filter(|path| is_format_source(path)) {
        let text = git(&["show", &format!("{tag}:{path}")])?;
        let idx = path
            .find("influxdb3_catalog/src/")
            .ok_or_else(|| format!("unexpected path from ls-tree: {path}"))?;
        sources.push((path[idx..].to_string(), text));
    }
    if sources.is_empty() {
        return Err(format!(
            "{tag} has no format sources: ls-tree listed {listed} paths, none of which \
             matched influxdb3_catalog/src/**/format/*.rs"
        ));
    }
    sources.sort();
    Ok(sources)
}

// ---------------------------------------------------------------------------
// Reference release
// ---------------------------------------------------------------------------

/// `major.minor.patch` of a released version, ignoring any pre-release suffix.
fn version(text: &str) -> Option<(u64, u64, u64)> {
    let mut parts = text.split('.');
    let numbers: Vec<u64> = [parts.next()?, parts.next()?, parts.next()?]
        .iter()
        .map(|part| part.parse().ok())
        .collect::<Option<_>>()?;
    parts
        .next()
        .is_none()
        .then(|| (numbers[0], numbers[1], numbers[2]))
}

/// Release tag names, from the checkout if it has any and from the remote if
/// it does not. A checkout made with `--no-tags` still resolves.
fn release_tags() -> Result<Vec<String>, String> {
    let local = git(&["tag", "--list", "v*"])?;
    if local.split_whitespace().next().is_some() {
        return Ok(local.split_whitespace().map(str::to_string).collect());
    }
    let remote = git(&["ls-remote", "--tags", "--refs", "origin", "v*"])?;
    Ok(remote
        .lines()
        .filter_map(|line| {
            Some(
                line.split('\t')
                    .nth(1)?
                    .strip_prefix("refs/tags/")?
                    .to_string(),
            )
        })
        .collect())
}

/// The newest final release tag older than the version being built.
///
/// Bounding by the crate's own version keeps this on the right line: `main`
/// compares against the newest release overall, and a release branch compares
/// against the newest tag of that line. It also skips the stray high-numbered
/// tags in this repository (`v99.99.9990` and friends), which a plain "newest
/// tag" would pick.
fn reference_tag() -> Result<String, String> {
    let building = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .and_then(version)
        .ok_or_else(|| {
            format!(
                "could not read a version from {}",
                env!("CARGO_PKG_VERSION")
            )
        })?;
    let tags = release_tags()?;
    tags.iter()
        .filter_map(|tag| {
            let parsed = version(tag.strip_prefix('v')?)?;
            (parsed < building).then(|| (parsed, tag.to_string()))
        })
        .max()
        .map(|(_, tag)| tag)
        .ok_or_else(|| {
            format!(
                "none of the {} tags git listed is a release older than {}.{}.{}",
                tags.len(),
                building.0,
                building.1,
                building.2
            )
        })
}

/// Make sure the commit behind `tag` is present, fetching that one tag if not.
///
/// A tag ref can be present while the objects it names are not: CircleCI's
/// `checkout` writes the refs but fetches only the branch, and release tags are
/// not ancestors of `main`. Fetching a single tag costs about fifty commits
/// beyond what the branch already carries, and leaves a shallow checkout
/// shallow.
fn ensure_tag_objects(tag: &str) -> Result<(), String> {
    let present = |tag: &str| git(&["cat-file", "-e", &format!("{tag}^{{commit}}")]).is_ok();
    if present(tag) {
        return Ok(());
    }
    git(&["fetch", "--no-recurse-submodules", "origin", "tag", tag])?;
    present(tag)
        .then_some(())
        .ok_or_else(|| format!("fetched {tag} but its commit is still missing"))
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Fails when a wire enum has grown out of the packing bucket it shipped in.
#[test]
fn wire_enums_have_not_crossed_a_packing_bucket() {
    let in_ci = std::env::var_os("CI").is_some();
    let skip = |reason: String| {
        assert!(!in_ci, "{reason} — this must work in CI");
        println!("did not run: {reason}");
    };

    let Some(worktree) = worktree_sources() else {
        return skip("no format sources under CARGO_MANIFEST_DIR/src".to_string());
    };
    let tag = match reference_tag() {
        Ok(tag) => tag,
        Err(reason) => return skip(reason),
    };
    if let Err(reason) = ensure_tag_objects(&tag) {
        return skip(reason);
    }
    let released = match tag_sources(&tag) {
        Ok(sources) => sources,
        Err(reason) => return skip(reason),
    };

    let current = enum_variant_counts(&worktree);
    let shipped = enum_variant_counts(&released);
    assert!(
        !current.is_empty() && !shipped.is_empty(),
        "found no bitcode enums under influxdb3_catalog/src/**/format — this scan is \
         looking in the wrong place, or the format moved"
    );

    let mut broken = String::new();
    for (name, &was) in &shipped {
        // A type introduced or removed since the reference release is not
        // constrained by it.
        let Some(&now) = current.get(name) else {
            continue;
        };
        if packing_bucket(was) != packing_bucket(now) {
            broken.push_str(&format!(
                "\n  {name}: {was} -> {now} variants, packing bucket _{} -> _{}",
                packing_bucket(was),
                packing_bucket(now)
            ));
        }
    }

    assert!(
        broken.is_empty(),
        "⚠ A catalog wire enum left the bitcode packing bucket it shipped in ⚠
{broken}

bitcode chooses how to pack an enum's variant tags from the variant count and
never writes that choice into the stream, so catalogs written by {tag} and
earlier will no longer decode — and may decode to different values without
reporting an error.

No edit to this enum makes it compatible again. The ways forward are:

  * add a new record type carrying a new enum, which is what the frozen-record
    rule in format::records expects, or
  * reuse a variant slot that is already deprecated, or
  * accept the break as part of a deliberate format version change.
"
    );
}
