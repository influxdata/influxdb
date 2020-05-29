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
  ## urls exp: http://127.0.0.1:9999
  urls = [%s]

  ## Token for authentication.
  token = "%s"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "%s"

  ## Destination bucket to write into.
  bucket = "%s"
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
