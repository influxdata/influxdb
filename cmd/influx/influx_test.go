package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"

	"github.com/google/go-cmp/cmp"
	influxtesting "github.com/influxdata/influxdb/testing"
)

type mockCli struct {
	Server      *httptest.Server
	ResponseMap map[string]methodResponse
	Cmd         *exec.Cmd
	StdOut      io.Writer
	StdErr      io.Writer
}

type mockHandler struct {
	// responseMap is the path to repsonse interface
	responseMap map[string]methodResponse
}

type methodResponse map[string]interface{}

func (h mockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mp, ok := h.responseMap[r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "404 not found")
		return
	}
	o, ok := mp[r.Method]
	if !ok {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	b, err := json.Marshal(o)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(b) == 0 || string(b) == "null" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Write(b)
}

func (m *mockCli) Start() {
	h := mockHandler{
		responseMap: m.ResponseMap,
	}
	m.Server = httptest.NewServer(h)
}

func (m *mockCli) Command(args ...string) *exec.Cmd {
	args = append([]string{"run", "../influx", "--host", m.Server.URL}, args...)
	cmd := exec.Command("go", args...)
	cmd.Stderr = m.StdErr
	cmd.Stdout = m.StdOut
	return cmd
}

func run(t *testing.T, m map[string]methodResponse, args []string, name string, want string) {
	buf := new(bytes.Buffer)
	cli := &mockCli{
		ResponseMap: m,
		StdErr:      buf,
		StdOut:      buf,
	}
	cli.Start()
	defer cli.Server.Close()
	cmd := cli.Command(args...)

	if err := cmd.Run(); err != nil {
		if diff := cmp.Diff(want, buf.String()); diff != "" {
			t.Errorf(name+" failed, output are different -got/+want\ndiff %s", diff)
		}
		return
	}
	if diff := cmp.Diff(want, buf.String()); diff != "" {
		t.Errorf(name+" failed, output are different -got/+want\ndiff %s", diff)
	}
}

var (
	oneID = influxtesting.MustIDBase16("020f755c3c082000")
	twoID = influxtesting.MustIDBase16("020f755c3c082001")
)

type setupResponse struct {
	Allowed bool `json:"allowed"`
}

var globalUsage = `
Global Flags:
      --host string    HTTP address of Influx (default "http://localhost:9999")
      --local          Run commands locally against the filesystem
  -t, --token string   API token to be used throughout client calls
`
var exit1 = `
exit status 1
`

func TestUsage(t *testing.T) {
	m := map[string]methodResponse{
		"/api/v2/setup": methodResponse{
			"GET": setupResponse{},
		},
	}
	usage := `Usage:
  influx [flags]
  influx [command]

Available Commands:
  auth        Authorization management commands
  bucket      Bucket management commands
  help        Help about any command
  org         Organization management commands
  ping        Check the InfluxDB /health endpoint
  query       Execute a Flux query
  repl        Interactive REPL (read-eval-print-loop)
  setup       Setup instance with initial user, org, bucket
  task        Task management commands
  user        User management commands
  write       Write points to InfluxDB

Flags:
  -h, --help           Help for the influx command 
      --host string    HTTP address of Influx (default "http://localhost:9999")
      --local          Run commands locally against the filesystem
  -t, --token string   API token to be used throughout client calls

Use "influx [command] --help" for more information about a command.
`
	run(t, m, []string{}, "", usage)
}
