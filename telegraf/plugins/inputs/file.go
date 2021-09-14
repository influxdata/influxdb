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
  ## Files to parse each interval.  Accept standard unix glob matching rules,
  ## as well as ** to match recursive files and directories.
  files = [%s]

  ## Name a tag containing the name of the file the data was parsed from.  Leave empty
  ## to disable.
  # file_tag = ""

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`, f.PluginName(), strings.Join(s, ", "))
}
