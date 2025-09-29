use influxdb3_lib::startup;

#[deny(clippy::disallowed_methods)]
fn main() -> Result<(), std::io::Error> {
    startup(std::env::args().collect())
}
