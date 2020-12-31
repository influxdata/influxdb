//! Implementation of command line option for manipulating and showing server
//! config

use config::Config;

pub fn show_config(ignore_config_file: bool) {
    let verbose = false;
    let config = load_config(verbose, ignore_config_file);
    println!("{}", config);
}

pub fn describe_config(ignore_config_file: bool) {
    let verbose = true;
    let config = load_config(verbose, ignore_config_file);
    println!("InfluxDB IOx Configuration:");
    println!("{}", config.verbose_display());
}

/// Loads the configuration information for IOx, and `abort`s the
/// process on error.
///
/// The rationale for panic is that anything wrong with the config is
/// should be fixed now rather then when it is used subsequently
///
/// If verbose is true, then messages are printed to stdout
pub fn load_config(verbose: bool, ignore_config_file: bool) -> Config {
    // Default configuraiton file is ~/.influxdb_iox/config
    let default_config_file = Config::default_config_file();

    // Try and create a useful error message / warnings
    let read_from_file = match (ignore_config_file, default_config_file.exists()) {
        // config exists but we got told to ignore if
        (true, true) => {
            println!("WARNING: Ignoring config file {:?}", default_config_file);
            false
        }
        // we got told to ignore the config file, but it didn't exist anyways
        (true, false) => {
            if verbose {
                println!(
                    "Loading config from environment (ignoring non existent file {:?})",
                    default_config_file
                );
            }
            false
        }
        (false, true) => {
            if verbose {
                println!(
                    "Loading config from file and environment (file: {:?})",
                    default_config_file
                );
            }
            true
        }
        (false, false) => {
            if verbose {
                println!(
                    "Loading config from environment (file: {:?} not found)",
                    default_config_file
                );
            }
            false
        }
    };

    //
    let config = if read_from_file {
        Config::try_from_path(&default_config_file)
    } else {
        Config::new_from_env()
    };

    match config {
        Err(e) => {
            eprintln!("FATAL Error loading config: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
        Ok(config) => config,
    }
}
