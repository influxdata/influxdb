package inputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Tail is based on telegraf Tail plugin.
type Tail struct {
	baseInput
	Files []string `json:"files"`
}

// PluginName is based on telegraf plugin name.
func (t *Tail) PluginName() string {
	return "tail"
}

// TOML encodes to toml string
func (t *Tail) TOML() string {
	s := make([]string, len(t.Files))
	for k, v := range t.Files {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.%s]]	
  ## files to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = [%s]

  ## Read file from beginning.
  from_beginning = false
  ## Whether file is a named pipe
  pipe = false
  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"
  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`, t.PluginName(), strings.Join(s, ", "))
}

// UnmarshalTOML decodes the parsed data to the object
func (t *Tail) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad files for tail input plugin")
	}
	files, ok := dataOK["files"].([]interface{})
	if !ok {
		return errors.New("not an array for tail input plugin")
	}
	for _, fi := range files {
		t.Files = append(t.Files, fi.(string))
	}
	return nil
}
