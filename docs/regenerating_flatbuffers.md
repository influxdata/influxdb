# Regenerating Flatbuffers code

If you have changed some `*.fbs` files:

- Run `./entry/regenerate-flatbuffers.sh` to regenerate the corresponding Rust code.
- Run `cargo test` to make sure everything works as you would expect.
- Check in the changes to the generated code along with your changes to the `*.fbs` files.
- You should not need to edit the `entry/regenerate-flatbuffers.sh` script.

If you are updating the version of the `flatbuffers` crate in `Cargo.lock`, either because a new
patch release has come out that is compatible with the version range in `Cargo.toml` and you have
run `cargo update`, or because you've updated the version constraint in `Cargo.toml` to change to a
new major or minor version:

- The `flatbuffers` crate gets developed in sync with the `flatc` compiler in the same repo,
  so when updating the `flatbuffers` crate we also need to update the `flatc` compiler we're
  using.
- Go to https://github.com/google/flatbuffers/blame/master/rust/flatbuffers/Cargo.toml and find
  the commit SHA where the `version` metadata was updated to the version of the `flatbuffers`
  crate we now want to have in our `Cargo.lock`.
- In the `entry/regenerate-flatbuffers.sh` script, put that commit SHA in the `FB_COMMIT`
  variable.
- Run `./entry/regenerate-flatbuffers.sh` to regenerate the corresponding Rust code.
- Run `cargo test` to make sure everything works as you would expect.
- Check in the changes to the generated code along with your changes to the `Cargo.lock` file,
  `Cargo.toml` file if relevant, and the `entry/regenerate-flatbuffers.sh` script.

By default, the `entry/regenerate-flatbuffers.sh` script will run a Docker container that
uses the same image we use in CI that will have all the necessary dependencies. If you don't want
to use Docker, run this script with `INFLUXDB_IOX_INTEGRATION_LOCAL=1`, which will require you to
have `bazel` available. You can likely install `bazel` with your favourite package manager.
