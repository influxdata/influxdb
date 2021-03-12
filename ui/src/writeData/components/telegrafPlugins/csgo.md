# Counter-Strike: Global Offensive (CSGO) Input Plugin

The `csgo` plugin gather metrics from Counter-Strike: Global Offensive servers.

#### Configuration
```toml
[[inputs.csgo]]
  ## Specify servers using the following format:
  ##    servers = [
  ##      ["ip1:port1", "rcon_password1"],
  ##      ["ip2:port2", "rcon_password2"],
  ##    ]
  #
  ## If no servers are specified, no data will be collected
  servers = []
```

### Metrics

The plugin retrieves the output of the `stats` command that is executed via rcon.

If no servers are specified, no data will be collected

- csgo
  - tags:
    - host
  - fields:
    - cpu (float)
    - net_in (float)
    - net_out (float)
    - uptime_minutes (float)
    - maps (float)
    - fps (float)
    - players (float)
    - sv_ms (float)
    - variance_ms (float)
    - tick_ms (float)
