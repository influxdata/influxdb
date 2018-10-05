package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// File is based on telegraf input File plugin.
type File struct {
	Files []string `json:"files"`
}

// TOML encodes to toml string
func (f *File) TOML() string {
	s := make([]string, len(f.Files))
	for k, v := range f.Files {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.file]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = [%s]
`, strings.Join(s, ", "))
}
