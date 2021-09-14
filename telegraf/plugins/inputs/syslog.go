package inputs

import (
	"errors"
	"fmt"
)

// Syslog is based on telegraf Syslog plugin.
type Syslog struct {
	baseInput
	Address string `json:"server"`
}

// PluginName is based on telegraf plugin name.
func (s *Syslog) PluginName() string {
	return "syslog"
}

// TOML encodes to toml string
func (s *Syslog) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = "%s"

  ## TLS Config
  # tls_allowed_cacerts = ["/etc/telegraf/ca.pem"]
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Period between keep alive probes.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # keep_alive_period = "5m"

  ## Maximum number of concurrent connections (default = 0).
  ## 0 means unlimited.
  ## Only applies to stream sockets (e.g. TCP).
  # max_connections = 1024

  ## Read timeout is the maximum time allowed for reading a single message (default = 5s).
  ## 0 means unlimited.
  # read_timeout = "5s"

  ## The framing technique with which it is expected that messages are transported (default = "octet-counting").
  ## Whether the messages come using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),
  ## or the non-transparent framing technique (RFC6587#section-3.4.2).
  ## Must be one of "octet-counting", "non-transparent".
  # framing = "octet-counting"

  ## The trailer to be expected in case of non-transparent framing (default = "LF").
  ## Must be one of "LF", or "NUL".
  # trailer = "LF"

  ## Whether to parse in best effort mode or not (default = false).
  ## By default best effort parsing is off.
  # best_effort = false

  ## Character to prepend to SD-PARAMs (default = "_").
  ## A syslog message can contain multiple parameters and multiple identifiers within structured data section.
  ## Eg., [id1 name1="val1" name2="val2"][id2 name1="val1" nameA="valA"]
  ## For each combination a field is created.
  ## Its name is created concatenating identifier, sdparam_separator, and parameter name.
  # sdparam_separator = "_"
`, s.PluginName(), s.Address)
}

// UnmarshalTOML decodes the parsed data to the object
func (s *Syslog) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad server for syslog input plugin")
	}
	s.Address, _ = dataOK["server"].(string)
	return nil
}
