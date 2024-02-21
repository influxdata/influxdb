use std::{
    net::{SocketAddrV4, TcpListener},
    process::{Child, Command, Stdio},
};

use arrow_flight::FlightClient;
use assert_cmd::cargo::CommandCargoExt;

pub struct TestServer {
    bind_addr: String,
    server_process: Child,
    http_client: reqwest::Client,
}

impl TestServer {
    pub async fn spawn() -> Self {
        let bind_addr = get_bind_addr();
        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let command = command
            .arg("serve")
            .args(["--http-bind", &bind_addr])
            // TODO - other configuration can be passed through
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let server_process = command.spawn().expect("spawn the influxdb3 server process");

        let server = Self {
            bind_addr,
            server_process,
            http_client: reqwest::Client::new(),
        };

        server.wait_until_ready().await;
        server
    }

    pub async fn wait_until_ready(&self) {
        while self
            .http_client
            .get(format!("{base}/health", base = self.client_addr()))
            .send()
            .await
            .is_err()
        {
            // TODO - sleep or do a count to ensure we don't loop infinitely
        }
    }

    pub fn client_addr(&self) -> String {
        format!("http://{addr}", addr = self.bind_addr)
    }

    pub fn kill(&mut self) {
        self.server_process.kill().expect("kill the server process");
    }

    pub async fn flight_client(&self) -> FlightClient {
        let channel = tonic::transport::Channel::from_shared(self.client_addr())
            .expect("create tonic channel")
            .connect()
            .await
            .expect("connect to gRPC client");
        FlightClient::new(channel)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.kill();
    }
}

fn get_bind_addr() -> String {
    let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);
    // Port 0 will find a free port
    let addr = SocketAddrV4::new(ip, 0).to_string();
    // Bind to get the full address with the selected port,
    // the TcpListener will be dropped at the end of this function
    // and therefore the port will be free for the command to start
    TcpListener::bind(addr)
        .expect("bind to a socket address")
        .local_addr()
        .expect("get local address")
        .to_string()
}
