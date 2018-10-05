package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// Prometheus is based on telegraf Prometheus plugin.
type Prometheus struct {
	URLs []string `json:"urls,omitempty"`
}

// TOML encodes to toml string
func (p *Prometheus) TOML() string {
	s := make([]string, len(p.URLs))
	for k, v := range s {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.prometheus]]	
  ## An array of urls to scrape metrics from.
  urls = [%s]
`, strings.Join(s, ", "))
}
