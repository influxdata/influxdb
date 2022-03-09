// The test in this file runs servers in separate threads and makes
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using [`ServerFixture`]
//
// The tests are defined in the submodules of [`end_to_end_ng_cases`]

pub mod common_ng;
mod end_to_end_ng_cases;
