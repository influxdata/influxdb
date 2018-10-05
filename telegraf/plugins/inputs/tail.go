package inputs

import (
	"fmt"
	"strconv"
	"strings"
)

// Tail is based on telegraf Tail plugin.
type Tail struct {
	Files []string `json:"files"`
}

// TOML encodes to toml string
func (t *Tail) TOML() string {
	s := make([]string, len(t.Files))
	for k, v := range s {
		s[k] = strconv.Quote(v)
	}
	return fmt.Sprintf(`[[inputs.tail]]	
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
`, strings.Join(s, ", "))
}
