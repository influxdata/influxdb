# Internal Types

This crate contains InfluxDB IOx "internal" types which are shared
across crates and internally between IOx instances, but not exposed
externally to clients

*Internal* in this case means that changing the structs is designed not to require additional coordination / organization with clients.
