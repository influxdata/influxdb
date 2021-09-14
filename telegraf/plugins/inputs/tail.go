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
  ## File names or a pattern to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##   "/var/log/log[!1-2]*  -> tail files without 1-2
  ##   "/var/log/log[^1-2]*  -> identical behavior as above
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = [%s]

  ## Read file from beginning.
  # from_beginning = false

  ## Whether file is a named pipe
  # pipe = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Maximum lines of the file to process that have not yet be written by the
  ## output.  For best throughput set based on the number of metrics on each
  ## line and the size of the output's metric_batch_size.
  # max_undelivered_lines = 1000

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

  ## Set the tag that will contain the path of the tailed file. If you don't want this tag, set it to an empty string.
  # path_tag = "path"

  ## multiline parser/codec
  ## https://www.elastic.co/guide/en/logstash/2.4/plugins-filters-multiline.html
  #[inputs.tail.multiline]
    ## The pattern should be a regexp which matches what you believe to be an
    ## indicator that the field is part of an event consisting of multiple lines of log data.
    #pattern = "^\s"

    ## This field must be either "previous" or "next".
    ## If a line matches the pattern, "previous" indicates that it belongs to the previous line,
    ## whereas "next" indicates that the line belongs to the next one.
    #match_which_line = "previous"

    ## The invert_match field can be true or false (defaults to false).
    ## If true, a message not matching the pattern will constitute a match of the multiline
    ## filter and the what will be applied. (vice-versa is also true)
    #invert_match = false

    ## After the specified timeout, this plugin sends a multiline event even if no new pattern
    ## is found to start a new event. The default timeout is 5s.
    #timeout = 5s
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
