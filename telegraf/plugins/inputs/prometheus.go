package inputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Prometheus is based on telegraf Prometheus plugin.
type Prometheus struct {
	baseInput
	URLs []string `json:"urls"`
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

// UnmarshalTOML decodes the parsed data to the object
func (p *Prometheus) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad urls for prometheus input plugin")
	}
	urls, ok := dataOK["urls"].([]interface{})
	if !ok {
		return errors.New("urls is not an array for prometheus input plugin")
	}
	for _, url := range urls {
		p.URLs = append(p.URLs, url.(string))
	}
	return nil
}
