package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// LogParserPlugin is based on telegraf LogParserPlugin.
type LogParserPlugin struct {
	baseInput
	Files []string `json:"files"`
}

// PluginName is based on telegraf plugin name.
func (l *LogParserPlugin) PluginName() string {
	return "logparser"
}

// TOML encodes to toml string
func (l *LogParserPlugin) TOML() string {
	s := make([]string, len(l.Files))
	for k, v := range l.Files {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.%s]]	
  ## Log files to parse.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only tail the apache log file
  files = [%s]
`, l.PluginName(), strings.Join(s, ", "))
}
