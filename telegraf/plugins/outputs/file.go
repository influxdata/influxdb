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
