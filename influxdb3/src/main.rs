use influxdb3_lib::startup;

fn main() -> Result<(), std::io::Error> {
    startup(std::env::args().collect())
}
