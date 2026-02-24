//! Config for socket addresses.
use std::{net::ToSocketAddrs, ops::Deref};

/// Parsable socket address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketAddr(std::net::SocketAddr);

impl Deref for SocketAddr {
    type Target = std::net::SocketAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for SocketAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for SocketAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_socket_addrs() {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.next() {
                    Ok(Self(addr))
                } else {
                    Err(format!("Found no addresses for '{s}'"))
                }
            }
            Err(e) => Err(format!("Cannot parse socket address '{s}': {e}")),
        }
    }
}

impl From<SocketAddr> for std::net::SocketAddr {
    fn from(addr: SocketAddr) -> Self {
        addr.0
    }
}

#[cfg(test)]
mod tests;
