package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// Redis is based on telegraf Redis plugin.
type Redis struct {
	Servers  []string `json:"servers"`
	Password string   `json:"password"`
}

// TOML encodes to toml string
func (r *Redis) TOML() string {
	s := make([]string, len(r.Servers))
	for k, v := range s {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.redis]]	
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

  ## specify server password
  # password = "%s"
`, strings.Join(s, ", "), r.Password)
}
