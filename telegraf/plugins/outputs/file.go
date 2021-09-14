package outputs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// File is based on telegraf file output plugin.
type File struct {
	baseOutput
	Files []FileConfig `json:"files"`
}

// FileConfig is the config settings of outpu file plugin.
type FileConfig struct {
	Typ  string `json:"type"`
	Path string `json:"path"`
}

// PluginName is based on telegraf plugin name.
func (f *File) PluginName() string {
	return "file"
}

// TOML encodes to toml string.
func (f *File) TOML() string {
	s := make([]string, len(f.Files))
	for k, v := range f.Files {
		if v.Typ == "stdout" {
			s[k] = strconv.Quote(v.Typ)
			continue
		}
		s[k] = strconv.Quote(v.Path)
	}
	return fmt.Sprintf(`[[outputs.%s]]
  ## Files to write to, "stdout" is a specially handled file.
  files = [%s]

  ## Use batch serialization format instead of line based delimiting.  The
  ## batch format allows for the production of non line based output formats and
  ## may more efficiently encode metric groups.
  # use_batch_format = false

  ## The file will be rotated after the time interval specified.  When set
  ## to 0 no time based rotation is performed.
  # rotation_interval = "0d"

  ## The logfile will be rotated when it becomes larger than the specified
  ## size.  When set to 0 no size based rotation is performed.
  # rotation_max_size = "0MB"

  ## Maximum number of rotated archives to keep, any older logs are deleted.
  ## If set to -1, no archives are removed.
  # rotation_max_archives = 5

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
`, f.PluginName(), strings.Join(s, ", "))
}

// UnmarshalTOML decodes the parsed data to the object
func (f *File) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad files for file output plugin")
	}
	files, ok := dataOK["files"].([]interface{})
	if !ok {
		return errors.New("not an array for file output plugin")
	}
	for _, fi := range files {
		fl := fi.(string)
		if fl == "stdout" {
			f.Files = append(f.Files, FileConfig{
				Typ: "stdout",
			})
			continue
		}
		f.Files = append(f.Files, FileConfig{
			Path: fl,
		})
	}
	return nil
}
