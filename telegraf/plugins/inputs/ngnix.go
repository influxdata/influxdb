package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// Nginx is based on telegraf nginx plugin.
type Nginx struct {
	URLs []string `json:"urls,omitempty"`
}

// TOML encodes to toml string
func (n *Nginx) TOML() string {
	s := make([]string, len(n.URLs))
	for k, v := range n.URLs {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.nginx]]
  # An array of Nginx stub_status URI to gather stats.
  # exp http://localhost/server_status
  urls = [%s]
`, strings.Join(s, ", "))
}
