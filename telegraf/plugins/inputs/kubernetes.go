package inputs

import (
	"errors"
	"fmt"
)

// Kubernetes is based on telegraf Kubernetes plugin
type Kubernetes struct {
	baseInput
	URL string `json:"url"`
}

// PluginName is based on telegraf plugin name.
func (k *Kubernetes) PluginName() string {
	return "kubernetes"
}

// TOML encodes to toml string.
func (k *Kubernetes) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## URL for the kubelet
  ## exp: http://1.1.1.1:10255
  url = "%s"	
`, k.PluginName(), k.URL)
}

// UnmarshalTOML decodes the parsed data to the object
func (k *Kubernetes) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad url for kubernetes input plugin")
	}
	k.URL, _ = dataOK["url"].(string)
	return nil
}
