// The test in this file runs the server in a separate thread and makes HTTP
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using [`ServerFixture`]
//
// The tests are defined in the submodules of [`end_to_end_cases`]

pub mod common;
mod end_to_end_cases;
