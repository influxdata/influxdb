package outputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// InfluxDBV2 is based on telegraf influxdb_v2 output plugin.
type InfluxDBV2 struct {
	baseOutput
	URLs         []string `json:"urls"`
	Token        string   `json:"token"`
	Organization string   `json:"organization"`
	Bucket       string   `json:"bucket"`
}

// PluginName is based on telegraf plugin name.
func (i *InfluxDBV2) PluginName() string {
	return "influxdb_v2"
}

// TOML encodes to toml string.
func (i *InfluxDBV2) TOML() string {
	s := make([]string, len(i.URLs))
	for k, v := range i.URLs {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[outputs.%s]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ##   ex: urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  urls = [%s]

  ## Token for authentication.
  token = "%s"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "%s"

  ## Destination bucket to write into.
  bucket = "%s"

  ## The value of this tag will be used to determine the bucket.  If this
  ## tag is not set the 'bucket' option is used as the default.
  # bucket_tag = ""

  ## If true, the bucket tag will not be added to the metric.
  # exclude_bucket_tag = false

  ## Timeout for HTTP messages.
  # timeout = "5s"

  ## Additional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## HTTP Proxy override, if unset values the standard proxy environment
  ## variables are consulted to determine which proxy, if any, should be used.
  # http_proxy = "http://corporate.proxy:3128"

  ## HTTP User-Agent
  # user_agent = "telegraf"

  ## Content-Encoding for write request body, can be set to "gzip" to
  ## compress body or "identity" to apply no encoding.
  # content_encoding = "gzip"

  ## Enable or disable uint support for writing uints influxdb 2.0.
  # influx_uint_support = false

  ## Optional TLS Config for use on HTTP connections.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`, i.PluginName(), strings.Join(s, ", "), i.Token, i.Organization, i.Bucket)
}

// UnmarshalTOML decodes the parsed data to the object
func (i *InfluxDBV2) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad urls for influxdb_v2 output plugin")
	}
	urls, ok := dataOK["urls"].([]interface{})
	if !ok {
		return errors.New("urls is not an array for influxdb_v2 output plugin")
	}
	for _, url := range urls {
		i.URLs = append(i.URLs, url.(string))
	}

	i.Token, ok = dataOK["token"].(string)
	if !ok {
		return errors.New("token is missing for influxdb_v2 output plugin")
	}

	i.Organization, ok = dataOK["organization"].(string)
	if !ok {
		return errors.New("organization is missing for influxdb_v2 output plugin")
	}

	i.Bucket, ok = dataOK["bucket"].(string)
	if !ok {
		return errors.New("bucket is missing for influxdb_v2 output plugin")
	}
	return nil
}
