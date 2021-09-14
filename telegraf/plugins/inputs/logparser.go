package inputs

import (
	"errors"
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

  ## Read files that currently exist from the beginning. Files that are created
  ## while telegraf is running (and that match the "files" globs) will always
  ## be read from the beginning.
  from_beginning = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Parse logstash-style "grok" patterns:
  [inputs.logparser.grok]
    ## This is a list of patterns to check the given log file(s) for.
    ## Note that adding patterns here increases processing time. The most
    ## efficient configuration is to have one pattern per logparser.
    ## Other common built-in patterns are:
    ##   %%{COMMON_LOG_FORMAT}   (plain apache & nginx access logs)
    ##   %%{COMBINED_LOG_FORMAT} (access logs + referrer & agent)
    patterns = ["%%{COMBINED_LOG_FORMAT}"]

    ## Name of the outputted measurement name.
    measurement = "apache_access_log"

    ## Full path(s) to custom pattern files.
    custom_pattern_files = []

    ## Custom patterns can also be defined here. Put one pattern per line.
    custom_patterns = '''
    '''

    ## Timezone allows you to provide an override for timestamps that
    ## don't already include an offset
    ## e.g. 04/06/2016 12:41:45 data one two 5.43µs
    ##
    ## Default: "" which renders UTC
    ## Options are as follows:
    ##   1. Local             -- interpret based on machine localtime
    ##   2. "Canada/Eastern"  -- Unix TZ values like those found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    ##   3. UTC               -- or blank/unspecified, will return timestamp in UTC
    # timezone = "Canada/Eastern"

      ## When set to "disable", timestamp will not incremented if there is a
      ## duplicate.
    # unique_timestamp = "auto"
`, l.PluginName(), strings.Join(s, ", "))
}

// UnmarshalTOML decodes the parsed data to the object
func (l *LogParserPlugin) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad files for logparser input plugin")
	}
	files, ok := dataOK["files"].([]interface{})
	if !ok {
		return errors.New("files is not an array for logparser input plugin")
	}
	for _, fi := range files {
		l.Files = append(l.Files, fi.(string))
	}
	return nil
}
