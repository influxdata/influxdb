package inputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// File is based on telegraf input File plugin.
type File struct {
	baseInput
	Files []string `json:"files"`
}

// PluginName is based on telegraf plugin name.
func (f *File) PluginName() string {
	return "file"
}

// UnmarshalTOML decodes the parsed data to the object
func (f *File) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad files for file input plugin")
	}
	files, ok := dataOK["files"].([]interface{})
	if !ok {
		return errors.New("not an array for file input plugin")
	}
	for _, fl := range files {
		f.Files = append(f.Files, fl.(string))
	}
	return nil
}

// TOML encodes to toml string
func (f *File) TOML() string {
	s := make([]string, len(f.Files))
	for k, v := range f.Files {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.%s]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = [%s]

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`, f.PluginName(), strings.Join(s, ", "))
}
