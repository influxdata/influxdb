package inputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Nginx is based on telegraf nginx plugin.
type Nginx struct {
	baseInput
	URLs []string `json:"urls"`
}

// PluginName is based on telegraf plugin name.
func (n *Nginx) PluginName() string {
	return "nginx"
}

// TOML encodes to toml string
func (n *Nginx) TOML() string {
	s := make([]string, len(n.URLs))
	for k, v := range n.URLs {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.%s]]
  # An array of Nginx stub_status URI to gather stats.
  # exp http://localhost/server_status
  urls = [%s]
`, n.PluginName(), strings.Join(s, ", "))
}

// UnmarshalTOML decodes the parsed data to the object
func (n *Nginx) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad urls for nginx input plugin")
	}
	urls, ok := dataOK["urls"].([]interface{})
	if !ok {
		return errors.New("urls is not an array for nginx input plugin")
	}
	for _, url := range urls {
		n.URLs = append(n.URLs, url.(string))
	}
	return nil
}
