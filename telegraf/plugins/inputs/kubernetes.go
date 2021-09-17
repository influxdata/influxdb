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
  url = "%s"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  ## If both of these are empty, we'll use the default serviceaccount:
  ## at: /run/secrets/kubernetes.io/serviceaccount/token
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## Pod labels to be added as tags.  An empty array for both include and
  ## exclude will include all labels.
  # label_include = []
  # label_exclude = ["*"]

  ## Set response_timeout (default 5 seconds)
  # response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`, k.PluginName(), k.URL)
}

// UnmarshalTOML decodes the parsed data to the object
func (k *Kubernetes) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad url for kubernetes input plugin")
	}
	if k.URL, ok = dataOK["url"].(string); !ok {
		return errors.New("url is not a string value")
	}
	return nil
}
