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

  ## Metric version controls the mapping from Prometheus metrics into
  ## Telegraf metrics.  When using the prometheus_client output, use the same
  ## value in both plugins to ensure metrics are round-tripped without
  ## modification.
  ##
  ##   example: metric_version = 1;
  ##            metric_version = 2; recommended version
  # metric_version = 1

  ## Url tag name (tag containing scrapped url. optional, default is "url")
  # url_tag = "url"

  ## An array of Kubernetes services to scrape metrics from.
  # kubernetes_services = ["http://my-service-dns.my-namespace:9100/metrics"]

  ## Kubernetes config file to create client from.
  # kube_config = "/path/to/kubernetes.config"

  ## Scrape Kubernetes pods for the following prometheus annotations:
  ## - prometheus.io/scrape: Enable scraping for this pod
  ## - prometheus.io/scheme: If the metrics endpoint is secured then you will need to
  ##     set this to 'https' & most likely set the tls config.
  ## - prometheus.io/path: If the metrics path is not /metrics, define it with this annotation.
  ## - prometheus.io/port: If port is not 9102 use this annotation
  # monitor_kubernetes_pods = true
  ## Get the list of pods to scrape with either the scope of
  ## - cluster: the kubernetes watch api (default, no need to specify)
  ## - node: the local cadvisor api; for scalability. Note that the config node_ip or the environment variable NODE_IP must be set to the host IP.
  # pod_scrape_scope = "cluster"
  ## Only for node scrape scope: node IP of the node that telegraf is running on.
  ## Either this config or the environment variable NODE_IP must be set.
  # node_ip = "10.180.1.1"
  # 	## Only for node scrape scope: interval in seconds for how often to get updated pod list for scraping.
  # 	## Default is 60 seconds.
  # 	# pod_scrape_interval = 60
  ## Restricts Kubernetes monitoring to a single namespace
  ##   ex: monitor_kubernetes_pods_namespace = "default"
  # monitor_kubernetes_pods_namespace = ""
  # label selector to target pods which have the label
  # kubernetes_label_selector = "env=dev,app=nginx"
  # field selector to target pods
  # eg. To scrape pods on a specific node
  # kubernetes_field_selector = "spec.nodeName=$HOSTNAME"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## HTTP Basic Authentication username and password. ('bearer_token' and
  ## 'bearer_token_string' take priority)
  # username = ""
  # password = ""

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
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
