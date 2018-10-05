package inputs

import "fmt"

// Kubernetes is based on telegraf Kubernetes plugin
type Kubernetes struct {
	URL string `json:"url"`
}

// TOML encodes to toml string.
func (k *Kubernetes) TOML() string {
	return fmt.Sprintf(`[[inputs.kubernetes]]
  ## URL for the kubelet
  ## exp: http://1.1.1.1:10255
  url = "%s"	
`, k.URL)
}
