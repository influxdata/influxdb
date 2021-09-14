package inputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Redis is based on telegraf Redis plugin.
type Redis struct {
	baseInput
	Servers  []string `json:"servers"`
	Password string   `json:"password"`
}

// PluginName is based on telegraf plugin name.
func (r *Redis) PluginName() string {
	return "redis"
}

// TOML encodes to toml string
func (r *Redis) TOML() string {
	s := make([]string, len(r.Servers))
	for k, v := range r.Servers {
		s[k] = strconv.Quote(v)
	}
	password := `  # password = ""`
	if r.Password != "" {
		password = fmt.Sprintf(`  # password = "%s"`, r.Password)
	}
	return fmt.Sprintf(`[[inputs.%s]]
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    tcp://localhost:6379
  ##    tcp://:password@192.168.99.100
  ##    unix:///var/run/redis.sock
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  servers = [%s]

  ## Optional. Specify redis commands to retrieve values
  # [[inputs.redis.commands]]
  #   # The command to run where each argument is a separate element
  #   command = ["get", "sample-key"]
  #   # The field to store the result in
  #   field = "sample-key-value"
  #   # The type of the result
  #   # Can be "string", "integer", or "float"
  #   type = "string"

  ## specify server password
%s

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = true
`, r.PluginName(), strings.Join(s, ", "), password)
}

// UnmarshalTOML decodes the parsed data to the object
func (r *Redis) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad servers for redis input plugin")
	}
	servers, ok := dataOK["servers"].([]interface{})
	if !ok {
		return errors.New("servers is not an array for redis input plugin")
	}
	for _, server := range servers {
		r.Servers = append(r.Servers, server.(string))
	}

	r.Password, _ = dataOK["password"].(string)

	return nil
}
