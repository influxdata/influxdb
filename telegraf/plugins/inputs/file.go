package inputs

import (
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
`, f.PluginName(), strings.Join(s, ", "))
}
