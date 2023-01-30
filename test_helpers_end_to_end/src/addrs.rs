use std::{
    fmt::Display,
    net::SocketAddrV4,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

// These port numbers are chosen to not collide with a development ioxd server
// running locally.
static NEXT_PORT: AtomicU16 = AtomicU16::new(8090);

// represents port on localhost to bind / connect to
#[derive(Debug, Clone)]
pub struct Address {
    /// the actual address, on which to bind. Example `127.0.0.1:8089`
    bind_addr: Arc<str>,
    /// address on which clients can connect. Example `http://127.0.0.1:8089`
    client_base: Arc<str>,
}

impl Address {
    fn new() -> Self {
        let bind_addr = Self::get_free_port().to_string();
        let client_base = format!("http://{bind_addr}");

        Self {
            bind_addr: bind_addr.into(),
            client_base: client_base.into(),
        }
    }
    fn get_free_port() -> SocketAddrV4 {
        let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);

        loop {
            let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
            let addr = SocketAddrV4::new(ip, port);

            if std::net::TcpListener::bind(addr).is_ok() {
                return addr;
            }
        }
    }

    pub fn bind_addr(&self) -> Arc<str> {
        Arc::clone(&self.bind_addr)
    }

    pub fn client_base(&self) -> Arc<str> {
        Arc::clone(&self.client_base)
    }
}

/// This structure contains all the addresses a test server could use
#[derive(Default, Debug)]
pub struct BindAddresses {
    router_http_api: std::sync::Mutex<Option<Address>>,
    router_grpc_api: std::sync::Mutex<Option<Address>>,
    querier_grpc_api: std::sync::Mutex<Option<Address>>,
    ingester_grpc_api: std::sync::Mutex<Option<Address>>,
    compactor_grpc_api: std::sync::Mutex<Option<Address>>,
}

impl BindAddresses {
    pub fn router_http_api(&self) -> Address {
        get_or_allocate(&self.router_http_api)
    }
    pub fn router_grpc_api(&self) -> Address {
        get_or_allocate(&self.router_grpc_api)
    }
    pub fn querier_grpc_api(&self) -> Address {
        get_or_allocate(&self.querier_grpc_api)
    }
    pub fn ingester_grpc_api(&self) -> Address {
        get_or_allocate(&self.ingester_grpc_api)
    }
    pub fn compactor_grpc_api(&self) -> Address {
        get_or_allocate(&self.compactor_grpc_api)
    }
}

impl Display for BindAddresses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(addr) = self.router_http_api.lock().unwrap().as_ref() {
            write!(f, "router_http: {} ", addr.bind_addr)?
        }
        if let Some(addr) = self.router_grpc_api.lock().unwrap().as_ref() {
            write!(f, "router_grpc: {} ", addr.bind_addr)?
        }
        if let Some(addr) = self.querier_grpc_api.lock().unwrap().as_ref() {
            write!(f, "querier_grpc: {} ", addr.bind_addr)?
        }
        if let Some(addr) = self.ingester_grpc_api.lock().unwrap().as_ref() {
            write!(f, "ingester_grpc: {} ", addr.bind_addr)?
        }
        if let Some(addr) = self.compactor_grpc_api.lock().unwrap().as_ref() {
            write!(f, "compactor_grpc: {} ", addr.bind_addr)?
        }
        Ok(())
    }
}

fn get_or_allocate(locked_addr: &std::sync::Mutex<Option<Address>>) -> Address {
    let mut locked_addr = locked_addr.lock().unwrap();
    let addr = locked_addr.take().unwrap_or_else(Address::new);
    *locked_addr = Some(addr.clone());
    addr
}
