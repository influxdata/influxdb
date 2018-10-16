package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// Prometheus is based on telegraf Prometheus plugin.
type Prometheus struct {
	baseInput
	URLs []string `json:"urls,omitempty"`
}

// PluginName is based on telegraf plugin name.
func (p *Prometheus) PluginName() string {
	return "prometheus"
}

// TOML encodes to toml string
func (p *Prometheus) TOML() string {
	s := make([]string, len(p.URLs))
	for k, v := range p.URLs {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.%s]]	
  ## An array of urls to scrape metrics from.
  urls = [%s]
`, p.PluginName(), strings.Join(s, ", "))
}
