// The test in this file runs servers in separate threads and makes
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using the code in [test_helpers_end_to_end]
//
// The tests are defined in the submodules of [`end_to_end_cases`]

mod end_to_end_cases;
mod query_tests;
