package inputs

import (
	"errors"
	"fmt"
)

// Procstat is based on telegraf procstat input plugin.
type Procstat struct {
	baseInput
	Exe string `json:"exe"`
}

// PluginName is based on telegraf plugin name.
func (p *Procstat) PluginName() string {
	return "procstat"
}

// TOML encodes to toml string.
func (p *Procstat) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## PID file to monitor process
  pid_file = "/var/run/nginx.pid"
  ## executable name (ie, pgrep <exe>)
  # exe = "%s"
  ## pattern as argument for pgrep (ie, pgrep -f <pattern>)
  # pattern = "nginx"
  ## user as argument for pgrep (ie, pgrep -u <user>)
  # user = "nginx"
  ## Systemd unit name
  # systemd_unit = "nginx.service"
  ## CGroup name or path
  # cgroup = "systemd/system.slice/nginx.service"

  ## Windows service name
  # win_service = ""

  ## override for process_name
  ## This is optional; default is sourced from /proc/<pid>/status
  # process_name = "bar"

  ## Field name prefix
  # prefix = ""

  ## When true add the full cmdline as a tag.
  # cmdline_tag = false

  ## Mode to use when calculating CPU usage. Can be one of 'solaris' or 'irix'.
  # mode = "irix"

  ## Add the PID as a tag instead of as a field.  When collecting multiple
  ## processes with otherwise matching tags this setting should be enabled to
  ## ensure each process has a unique identity.
  ##
  ## Enabling this option may result in a large number of series, especially
  ## when processes have a short lifetime.
  # pid_tag = false

  ## Method to use when finding process IDs.  Can be one of 'pgrep', or
  ## 'native'.  The pgrep finder calls the pgrep executable in the PATH while
  ## the native finder performs the search directly in a manor dependent on the
  ## platform.  Default is 'pgrep'
  # pid_finder = "pgrep"
`, p.PluginName(), p.Exe)
}

// UnmarshalTOML decodes the parsed data to the object
func (p *Procstat) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad exe for procstat input plugin")
	}
	if p.Exe, ok = dataOK["exe"].(string); !ok {
		return errors.New("exe is not a string value")
	}
	return nil
}
