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
mod tests {
    use super::*;

    use std::{
        net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
        str::FromStr,
    };

    #[test]
    fn test_socketaddr() {
        let addr: std::net::SocketAddr = SocketAddr::from_str("127.0.0.1:1234").unwrap().into();
        assert_eq!(addr, std::net::SocketAddr::from(([127, 0, 0, 1], 1234)),);

        let addr: std::net::SocketAddr = SocketAddr::from_str("localhost:1234").unwrap().into();
        // depending on where the test runs, localhost will either resolve to a ipv4 or
        // an ipv6 addr.
        match addr {
            std::net::SocketAddr::V4(so) => {
                assert_eq!(so, SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1234))
            }
            std::net::SocketAddr::V6(so) => assert_eq!(
                so,
                SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), 1234, 0, 0)
            ),
        };

        assert_eq!(
            SocketAddr::from_str("!@INv_a1d(ad0/resp_!").unwrap_err(),
            "Cannot parse socket address '!@INv_a1d(ad0/resp_!': invalid socket address",
        );
    }
}
