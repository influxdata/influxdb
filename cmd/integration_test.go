package cmd_test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/httpd"
)

type TestRunCommand struct {
	*run.Command

	// Temporary directory used for default data, meta, and wal dirs.
	Dir string
}

func NewTestRunCommand(env map[string]string) *TestRunCommand {
	dir, err := ioutil.TempDir("", "testrun-")
	if err != nil {
		panic(err)
	}

	cmd := run.NewCommand()
	cmd.Getenv = func(k string) string {
		// Return value in env map, if set.
		var v string
		if env != nil {
			v = env[k]
		}
		if v != "" {
			return v
		}

		// If the key wasn't explicitly set in env, use some reasonable defaults for test.
		switch k {
		case "INFLUXDB_DATA_DIR":
			return filepath.Join(dir, "data")
		case "INFLUXDB_META_DIR":
			return filepath.Join(dir, "meta")
		case "INFLUXDB_WAL_DIR":
			return filepath.Join(dir, "wal")
		case "INFLUXDB_HTTP_BIND_ADDRESS":
			return "localhost:0"
		case "INFLUXDB_BIND_ADDRESS":
			return "localhost:0"
		case "INFLUXDB_REPORTING_DISABLED":
			return "true"
		default:
			return ""
		}
	}

	return &TestRunCommand{
		Command: cmd,
		Dir:     dir,
	}
}

// MustRun calls Command.Run and panics if there is an error.
func (c *TestRunCommand) MustRun() {
	if err := c.Command.Run(); err != nil {
		panic(err)
	}
}

// HTTPClient returns a new v2 HTTP client.
func (c *TestRunCommand) HTTPClient() client.Client {
	cl, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://" + c.HTTPAddr(),
	})
	if err != nil {
		panic(err)
	}
	return cl
}

// HTTPAddr returns the bind address of the HTTP service, in form "localhost:65432".
func (c *TestRunCommand) HTTPAddr() string {
	for _, s := range c.Command.Server.Services {
		if s, ok := s.(*httpd.Service); ok {
			return s.HTTPAddr()
		}
	}
	panic("Did not find HTTPD service!")
}

func (c *TestRunCommand) Cleanup() {
	c.Command.Close()
	os.RemoveAll(c.Dir)
}
