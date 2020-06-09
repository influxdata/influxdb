mod server;

mod rpc;

fn main() {
    println!("Staring delorean server...");
    match server::main() {
        Ok(()) => eprintln!("Shutdown OK"),
        Err(e) => {
            eprintln!("Exiting with error: {:?}", e);
        }
    }
}
