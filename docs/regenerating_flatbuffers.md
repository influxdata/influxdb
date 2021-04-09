# Regenerating Flatbuffers code

When updating the version of the [flatbuffers](https://crates.io/crates/flatbuffers) Rust crate used as a dependency in the IOx workspace, the generated Rust code in `generated_types/src/wal_generated.rs` also needs to be updated in sync.

To update the generated code, edit `generated_types/regenerate-flatbuffers.sh` and set the `FB_COMMIT` variable at the top of the file to the commit SHA of the same commit in the [flatbuffers repository](https://github.com/google/flatbuffers) where the `flatbuffers` Rust crate version was updated. This ensures we'll be [using the same version of `flatc` that the crate was tested with](https://github.com/google/flatbuffers/issues/6199#issuecomment-714562121).

Then run the `generated_types/regenerate-flatbuffers.sh` script and check in any changes. Check the whole project builds.
