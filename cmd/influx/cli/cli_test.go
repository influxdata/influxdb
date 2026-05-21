package cli_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/client"
	"github.com/influxdata/influxdb/cmd/influx/cli"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"github.com/peterh/liner"
	"github.com/stretchr/testify/require"
)

const (
	CLIENT_VERSION = "y.y"
	SERVER_VERSION = "x.x"
)

func TestNewCLI(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)

	if c == nil {
		t.Fatal("CommandLine shouldn't be nil.")
	}

	if c.ClientVersion != CLIENT_VERSION {
		t.Fatalf("CommandLine version is %s but should be %s", c.ClientVersion, CLIENT_VERSION)
	}
}

func TestRunCLI_ExecuteInsert(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	h, p, _ := net.SplitHostPort(u.Host)
	c := cli.New(CLIENT_VERSION)
	c.Host = h
	c.Port, _ = strconv.Atoi(p)
	c.ClientConfig.Precision = "ms"
	c.Execute = "INSERT sensor,floor=1 value=2"
	c.IgnoreSignals = true
	c.ForceTTY = true
	if err := c.Run(); err != nil {
		t.Fatalf("Run failed with error: %s", err)
	}
}

func TestRunCLI_ExecuteInsertWithPath(t *testing.T) {
	path := "boom"
	t.Parallel()
	ts := emptyTestServerWithPath(path)
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	h, p, _ := net.SplitHostPort(u.Host)
	c := cli.New(CLIENT_VERSION)
	c.Host = h
	c.PathPrefix = path
	c.Port, _ = strconv.Atoi(p)
	c.ClientConfig.Precision = "ms"
	c.Execute = "INSERT sensor,floor=1 value=2"
	c.IgnoreSignals = true
	c.ForceTTY = true
	if err := c.Run(); err != nil {
		t.Fatalf("Run failed with error: %s", err)
	}
}

func TestRunCLI_ExecuteInsert_WithSignals(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	h, p, _ := net.SplitHostPort(u.Host)
	c := cli.New(CLIENT_VERSION)
	c.Host = h
	c.Port, _ = strconv.Atoi(p)
	c.ClientConfig.Precision = "ms"
	c.Execute = "INSERT sensor,floor=1 value=2"
	c.IgnoreSignals = false
	c.ForceTTY = true
	if err := c.Run(); err != nil {
		t.Fatalf("Run failed with error: %s", err)
	}
}

func TestSetAuth(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client
	u := "userx"
	p := "pwdy"
	c.SetAuth("auth " + u + " " + p)

	// validate CLI configuration
	if c.ClientConfig.Username != u {
		t.Fatalf("Username is %s but should be %s", c.ClientConfig.Username, u)
	}
	if c.ClientConfig.Password != p {
		t.Fatalf("Password is %s but should be %s", c.ClientConfig.Password, p)
	}
}

func TestSetPrecision(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client

	// validate set non-default precision
	p := "ns"
	c.SetPrecision("precision " + p)
	if c.ClientConfig.Precision != p {
		t.Fatalf("Precision is %s but should be %s", c.ClientConfig.Precision, p)
	}
	up := "NS"
	c.SetPrecision("PRECISION " + up)
	if c.ClientConfig.Precision != p {
		t.Fatalf("Precision is %s but should be %s", c.ClientConfig.Precision, p)
	}
	mixed := "ns"
	c.SetPrecision("PRECISION " + mixed)
	if c.ClientConfig.Precision != p {
		t.Fatalf("Precision is %s but should be %s", c.ClientConfig.Precision, p)
	}

	// validate set default precision which equals empty string
	p = "rfc3339"
	c.SetPrecision("precision " + p)
	if c.ClientConfig.Precision != "" {
		t.Fatalf("Precision is %s but should be empty", c.ClientConfig.Precision)
	}
	p = "RFC3339"
	c.SetPrecision("precision " + p)
	if c.ClientConfig.Precision != "" {
		t.Fatalf("Precision is %s but should be empty", c.ClientConfig.Precision)
	}
}

func TestSetFormat(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client

	// validate set non-default format
	f := "json"
	c.SetFormat("format " + f)
	if c.Format != f {
		t.Fatalf("Format is %s but should be %s", c.Format, f)
	}

	uf := "JSON"
	c.SetFormat("format " + uf)
	if c.Format != f {
		t.Fatalf("Format is %s but should be %s", c.Format, f)
	}
	mixed := "json"
	c.SetFormat("FORMAT " + mixed)
	if c.Format != f {
		t.Fatalf("Format is %s but should be %s", c.Format, f)
	}
}

func Test_SetChunked(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client

	// make sure chunked is on by default
	if got, exp := c.Chunked, true; got != exp {
		t.Fatalf("chunked should be on by default.  got %v, exp %v", got, exp)
	}

	// turn chunked off
	if err := c.ParseCommand("Chunked"); err != nil {
		t.Fatalf("setting chunked failed: err: %s", err)
	}

	if got, exp := c.Chunked, false; got != exp {
		t.Fatalf("setting chunked failed.  got %v, exp %v", got, exp)
	}

	// turn chunked back on
	if err := c.ParseCommand("Chunked"); err != nil {
		t.Fatalf("setting chunked failed: err: %s", err)
	}

	if got, exp := c.Chunked, true; got != exp {
		t.Fatalf("setting chunked failed.  got %v, exp %v", got, exp)
	}
}

func Test_SetChunkSize(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client

	// check default chunk size
	if got, exp := c.ChunkSize, 0; got != exp {
		t.Fatalf("unexpected chunk size.  got %d, exp %d", got, exp)
	}

	tests := []struct {
		command string
		exp     int
	}{
		{"chunk size 20", 20},
		{"   CHunk     siZE  55    ", 55},
		{"chunk 10", 10},
		{"     chuNK     15", 15},
		{"chunk size -60", 0},
		{"chunk size 10", 10},
		{"chunk size 0", 0},
		{"chunk size 10", 10},
		{"chunk size junk", 10},
	}

	for _, test := range tests {
		if err := c.ParseCommand(test.command); err != nil {
			t.Logf("command: %q", test.command)
			t.Fatalf("setting chunked failed: err: %s", err)
		}

		if got, exp := c.ChunkSize, test.exp; got != exp {
			t.Logf("command: %q", test.command)
			t.Fatalf("unexpected chunk size.  got %d, exp %d", got, exp)
		}
	}
}

func TestSetWriteConsistency(t *testing.T) {
	t.Parallel()
	c := cli.New(CLIENT_VERSION)
	config := client.NewConfig()
	client, _ := client.NewClient(config)
	c.Client = client

	// set valid write consistency
	consistency := "all"
	c.SetWriteConsistency("consistency " + consistency)
	if c.ClientConfig.WriteConsistency != consistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.ClientConfig.WriteConsistency, consistency)
	}

	// set different valid write consistency and validate change
	consistency = "quorum"
	c.SetWriteConsistency("consistency " + consistency)
	if c.ClientConfig.WriteConsistency != consistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.ClientConfig.WriteConsistency, consistency)
	}

	consistency = "QUORUM"
	c.SetWriteConsistency("consistency " + consistency)
	if c.ClientConfig.WriteConsistency != "quorum" {
		t.Fatalf("WriteConsistency is %s but should be %s", c.ClientConfig.WriteConsistency, "quorum")
	}

	consistency = "quorum"
	c.SetWriteConsistency("CONSISTENCY " + consistency)
	if c.ClientConfig.WriteConsistency != consistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.ClientConfig.WriteConsistency, consistency)
	}

	// set invalid write consistency and verify there was no change
	invalidConsistency := "invalid_consistency"
	c.SetWriteConsistency("consistency " + invalidConsistency)
	if c.ClientConfig.WriteConsistency == invalidConsistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.ClientConfig.WriteConsistency, consistency)
	}
}

func TestParseCommand_CommandsExist(t *testing.T) {
	t.Parallel()
	c, err := client.NewClient(client.Config{})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	m := cli.CommandLine{Client: c, Line: liner.NewLiner()}
	tests := []struct {
		cmd string
	}{
		{cmd: "gopher"},
		{cmd: "auth"},
		{cmd: "help"},
		{cmd: "format"},
		{cmd: "precision"},
		{cmd: "settings"},
	}
	for _, test := range tests {
		if err := m.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil`, err, test.cmd)
		}
	}
}

func TestParseCommand_Connect(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	cmd := "connect " + u.Host
	c := cli.CommandLine{}

	// assert connection is established
	if err := c.ParseCommand(cmd); err != nil {
		t.Fatalf("There was an error while connecting to %v: %v", u.Path, err)
	}

	// assert server version is populated
	if c.ServerVersion != SERVER_VERSION {
		t.Fatalf("Server version is %s but should be %s.", c.ServerVersion, SERVER_VERSION)
	}
}

func TestParseCommand_TogglePretty(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{}
	if c.Pretty {
		t.Fatalf(`Pretty should be false.`)
	}
	c.ParseCommand("pretty")
	if !c.Pretty {
		t.Fatalf(`Pretty should be true.`)
	}
	c.ParseCommand("pretty")
	if c.Pretty {
		t.Fatalf(`Pretty should be false.`)
	}
}

func TestParseCommand_Exit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cmd string
	}{
		{cmd: "exit"},
		{cmd: " exit"},
		{cmd: "exit "},
		{cmd: "Exit "},
	}

	for _, test := range tests {
		c := cli.CommandLine{Quit: make(chan struct{}, 1)}
		c.ParseCommand(test.cmd)
		// channel should be closed
		if _, ok := <-c.Quit; ok {
			t.Fatalf(`Command "exit" failed for %q.`, test.cmd)
		}
	}
}

func TestParseCommand_Quit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cmd string
	}{
		{cmd: "quit"},
		{cmd: " quit"},
		{cmd: "quit "},
		{cmd: "Quit "},
	}

	for _, test := range tests {
		c := cli.CommandLine{Quit: make(chan struct{}, 1)}
		c.ParseCommand(test.cmd)
		// channel should be closed
		if _, ok := <-c.Quit; ok {
			t.Fatalf(`Command "quit" failed for %q.`, test.cmd)
		}
	}
}

func TestParseCommand_Use(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	config := client.Config{URL: *u}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	tests := []struct {
		cmd string
		db  string
	}{
		{cmd: "use db", db: "db"},
		{cmd: " use db", db: "db"},
		{cmd: "use db ", db: "db"},
		{cmd: "use db;", db: "db"},
		{cmd: "use db; ", db: "db"},
		{cmd: "Use db", db: "db"},
		{cmd: `Use "db"`, db: "db"},
		{cmd: `Use "db db"`, db: "db db"},
	}

	for _, test := range tests {
		m := cli.CommandLine{Client: c}
		if err := m.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}

		if m.Database != test.db {
			t.Fatalf(`Command "%s" changed database to %q. Expected %s`, test.cmd, m.Database, test.db)
		}
	}
}

func TestParseCommand_UseAuth(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	tests := []struct {
		cmd      string
		user     string
		database string
	}{
		{
			cmd:      "use db",
			user:     "admin",
			database: "db",
		},
		{
			cmd:      "use blank",
			user:     "admin",
			database: "",
		},
		{
			cmd:      "use db",
			user:     "anonymous",
			database: "db",
		},
		{
			cmd:      "use blank",
			user:     "anonymous",
			database: "blank",
		},
	}

	for i, tt := range tests {
		config := client.Config{URL: *u, Username: tt.user}
		fmt.Println("using auth:", tt.user)
		c, err := client.NewClient(config)
		if err != nil {
			t.Errorf("%d. unexpected error.  expected %v, actual %v", i, nil, err)
			continue
		}
		m := cli.CommandLine{Client: c}
		m.ClientConfig.Username = tt.user

		if err := m.ParseCommand(tt.cmd); err != nil {
			t.Fatalf(`%d. Got error %v for command %q, expected nil.`, i, err, tt.cmd)
		}

		if m.Database != tt.database {
			t.Fatalf(`%d. Command "use" changed database to %q. Expected %q`, i, m.Database, tt.database)
		}
	}
}

func TestParseCommand_Consistency(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{}
	tests := []struct {
		cmd string
	}{
		{cmd: "consistency one"},
		{cmd: " consistency one"},
		{cmd: "consistency one "},
		{cmd: "consistency one;"},
		{cmd: "consistency one; "},
		{cmd: "Consistency one"},
	}

	for _, test := range tests {
		if err := c.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}

		if c.ClientConfig.WriteConsistency != "one" {
			t.Fatalf(`Command "consistency" changed consistency to %q. Expected one`, c.ClientConfig.WriteConsistency)
		}
	}
}

func TestParseCommand_Insert(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	config := client.Config{URL: *u}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
	m := cli.CommandLine{Client: c}

	tests := []struct {
		cmd string
	}{
		{cmd: "INSERT cpu,host=serverA,region=us-west value=1.0"},
		{cmd: " INSERT cpu,host=serverA,region=us-west value=1.0"},
		{cmd: "INSERT   cpu,host=serverA,region=us-west value=1.0"},
		{cmd: "insert cpu,host=serverA,region=us-west    value=1.0    "},
		{cmd: "insert"},
		{cmd: "Insert "},
		{cmd: "insert c"},
		{cmd: "insert int"},
	}

	for _, test := range tests {
		if err := m.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}
	}
}

func TestParseCommand_History(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{Line: liner.NewLiner()}
	defer c.Line.Close()

	// append one entry to history
	c.Line.AppendHistory("abc")

	tests := []struct {
		cmd string
	}{
		{cmd: "history"},
		{cmd: " history"},
		{cmd: "history "},
		{cmd: "History "},
	}

	for _, test := range tests {
		if err := c.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}
	}

	// buf size should be at least 1
	var buf bytes.Buffer
	c.Line.WriteHistory(&buf)
	if buf.Len() < 1 {
		t.Fatal("History is borked")
	}
}

func TestParseCommand_HistoryWithBlankCommand(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{Line: liner.NewLiner()}
	defer c.Line.Close()

	// append one entry to history
	c.Line.AppendHistory("x")

	tests := []struct {
		cmd string
		err error
	}{
		{cmd: "history"},
		{cmd: " history"},
		{cmd: "history "},
		{cmd: "", err: cli.ErrBlankCommand},      // shouldn't be persisted in history
		{cmd: " ", err: cli.ErrBlankCommand},     // shouldn't be persisted in history
		{cmd: "     ", err: cli.ErrBlankCommand}, // shouldn't be persisted in history
	}

	// a blank command will return cli.ErrBlankCommand.
	for _, test := range tests {
		if err := c.ParseCommand(test.cmd); err != test.err {
			t.Errorf(`Got error %v for command %q, expected %v`, err, test.cmd, test.err)
		}
	}

	// buf shall not contain empty commands
	var buf bytes.Buffer
	c.Line.WriteHistory(&buf)
	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			t.Fatal("Empty commands should not be persisted in history.")
		}
	}
}

func emptyTestServerWithPath(path string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Influxdb-Version", SERVER_VERSION)

		// Fake authorization entirely based on the username.
		authorized := false
		user, _, _ := r.BasicAuth()
		switch user {
		case "", "admin":
			authorized = true
		}

		queryPath := path + "/query"
		writePath := path + "/write"

		switch r.URL.Path {
		case queryPath:
			values := r.URL.Query()
			parser := influxql.NewParser(bytes.NewBufferString(values.Get("q")))
			q, err := parser.ParseQuery()
			if err != nil {
				return
			}
			stmt := q.Statements[0]

			switch stmt.(type) {
			case *influxql.ShowDatabasesStatement:
				if authorized {
					io.WriteString(w, `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db", "db db"]]}]}]}`)
				} else {
					w.WriteHeader(http.StatusUnauthorized)
					io.WriteString(w, fmt.Sprintf(`{"error":"error authorizing query: %s not authorized to execute statement 'SHOW DATABASES', requires admin privilege"}`, user))
				}
			case *influxql.ShowDiagnosticsStatement:
				io.WriteString(w, `{"results":[{}]}`)
			}
		case writePath:
			w.WriteHeader(http.StatusOK)
		}
	}))
}

// Shared fixture values for the two PartialMeasurements tests below.
const (
	formatColumn = "column"
	formatJSON   = "json"

	levelWarning       = "warning"
	levelWarningPrefix = levelWarning + ": "

	seriesNameMeasurements    = "measurements"
	seriesNameHeaderColumnFmt = "name: " + seriesNameMeasurements

	colNameField       = "name"
	colDatabase        = "database"
	colRetentionPolicy = "retention policy"

	measurementCPU = "cpu"
	measurementMem = "mem"

	dbName0 = "db0"
	dbName1 = "db1"
	rpName0 = "rp0"

	warnTextSingleDB0 = `partial results for "db0": node 2: timeout`
	warnTextDB0RP0    = `partial results for "db0"."rp0": db0: timeout`
	warnTextDB1RP0    = `partial results for "db1"."rp0": db1: refused`
)

// TestCommandLine_FormatResponse_Column_PartialMeasurements verifies that
// when SHOW MEASUREMENTS returns partial results — some measurement rows plus
// one or more warning Messages produced by partialMeasurementsWarning in the
// coordinator — the influx CLI's "column" formatter prints both pieces in the
// expected shape: each message rendered as "<level>: <text>." on its own line
// before the table, followed by the column header, a dashed underline, and the
// data rows.
func TestCommandLine_FormatResponse_Column_PartialMeasurements(t *testing.T) {
	t.Parallel()

	// findLine returns the index of the first line in lines that contains sub,
	// or -1. Used to assert relative ordering without depending on exact
	// whitespace produced by tabwriter.
	findLine := func(lines []string, sub string) int {
		for i, l := range lines {
			if strings.Contains(l, sub) {
				return i
			}
		}
		return -1
	}

	t.Run("single_source_one_warning", func(t *testing.T) {
		t.Parallel()
		resp := &client.Response{
			Results: []client.Result{{
				Messages: []*client.Message{{
					Level: levelWarning,
					Text:  warnTextSingleDB0,
				}},
				Series: []models.Row{{
					Name:    seriesNameMeasurements,
					Columns: []string{colNameField},
					Values:  [][]interface{}{{measurementCPU}, {measurementMem}},
				}},
			}},
		}

		c := cli.New(CLIENT_VERSION)
		c.Format = formatColumn
		var buf bytes.Buffer
		c.FormatResponse(resp, &buf)

		out := buf.String()
		lines := strings.Split(strings.TrimRight(out, "\n"), "\n")

		warnIdx := findLine(lines, levelWarningPrefix)
		require.NotEqual(t, -1, warnIdx, "expected a warning line in output:\n%s", out)
		require.Equal(t, levelWarningPrefix+warnTextSingleDB0+".", strings.TrimRight(lines[warnIdx], " "),
			"warning line should be rendered as '<level>: <text>.' with the CLI-appended period")

		nameHdrIdx := findLine(lines, seriesNameHeaderColumnFmt)
		require.NotEqual(t, -1, nameHdrIdx, "expected series name header line:\n%s", out)
		colHdrIdx := findLine(lines[nameHdrIdx+1:], colNameField)
		require.NotEqual(t, -1, colHdrIdx, "expected 'name' column header line:\n%s", out)
		colHdrIdx += nameHdrIdx + 1
		dashIdx := colHdrIdx + 1
		require.Less(t, dashIdx, len(lines), "expected a dashed underline after the column header")
		require.Equal(t, "----", strings.TrimSpace(lines[dashIdx]),
			"column underline should match the width of 'name'")

		cpuIdx := findLine(lines, measurementCPU)
		memIdx := findLine(lines, measurementMem)
		require.NotEqual(t, -1, cpuIdx)
		require.NotEqual(t, -1, memIdx)

		require.Less(t, warnIdx, nameHdrIdx, "warning must precede the table")
		require.Less(t, dashIdx, cpuIdx, "data rows must come after the dashed underline")
		require.Less(t, cpuIdx, memIdx, "data rows preserve series order")
	})

	t.Run("wildcard_two_warnings_three_columns", func(t *testing.T) {
		t.Parallel()
		resp := &client.Response{
			Results: []client.Result{{
				Messages: []*client.Message{
					{Level: levelWarning, Text: warnTextDB0RP0},
					{Level: levelWarning, Text: warnTextDB1RP0},
				},
				Series: []models.Row{{
					Name:    seriesNameMeasurements,
					Columns: []string{colNameField, colDatabase, colRetentionPolicy},
					Values: [][]interface{}{
						{measurementCPU, dbName0, rpName0},
						{measurementMem, dbName1, rpName0},
					},
				}},
			}},
		}

		c := cli.New(CLIENT_VERSION)
		c.Format = formatColumn
		var buf bytes.Buffer
		c.FormatResponse(resp, &buf)

		out := buf.String()
		lines := strings.Split(strings.TrimRight(out, "\n"), "\n")

		w0 := findLine(lines, warnTextDB0RP0+".")
		w1 := findLine(lines, warnTextDB1RP0+".")
		require.NotEqual(t, -1, w0, "expected first warning line:\n%s", out)
		require.NotEqual(t, -1, w1, "expected second warning line:\n%s", out)
		require.True(t, strings.HasPrefix(strings.TrimSpace(lines[w0]), levelWarningPrefix),
			"first warning should carry the 'warning: ' level prefix; got: %q", lines[w0])
		require.True(t, strings.HasPrefix(strings.TrimSpace(lines[w1]), levelWarningPrefix),
			"second warning should carry the 'warning: ' level prefix; got: %q", lines[w1])
		require.Less(t, w0, w1, "warnings preserved in coordinator order")

		nameHdrIdx := findLine(lines, seriesNameHeaderColumnFmt)
		require.NotEqual(t, -1, nameHdrIdx, "expected series name header line:\n%s", out)

		// Column header line must list all three columns in order.
		colHdrIdx := -1
		for i := nameHdrIdx + 1; i < len(lines); i++ {
			l := lines[i]
			if strings.Contains(l, colNameField) && strings.Contains(l, colDatabase) && strings.Contains(l, colRetentionPolicy) {
				colHdrIdx = i
				break
			}
		}
		require.NotEqual(t, -1, colHdrIdx, "expected a three-column header line:\n%s", out)
		require.Less(t, strings.Index(lines[colHdrIdx], colNameField), strings.Index(lines[colHdrIdx], colDatabase),
			"column order is name | database | retention policy")
		require.Less(t, strings.Index(lines[colHdrIdx], colDatabase), strings.Index(lines[colHdrIdx], colRetentionPolicy),
			"column order is name | database | retention policy")

		// Dashed underline: lengths must match each column header exactly
		// (len("name")=4, len("database")=8, len("retention policy")=16).
		dashIdx := colHdrIdx + 1
		require.Less(t, dashIdx, len(lines), "expected dashed underline after column header")
		dashFields := strings.Fields(lines[dashIdx])
		require.Equal(t, []string{
			strings.Repeat("-", len(colNameField)),
			strings.Repeat("-", len(colDatabase)),
			strings.Repeat("-", len(colRetentionPolicy)),
		}, dashFields, "each column's underline width matches its header width")

		// Data rows present, after the underline, in series order.
		cpuIdx := findLine(lines, measurementCPU)
		memIdx := findLine(lines, measurementMem)
		require.NotEqual(t, -1, cpuIdx)
		require.NotEqual(t, -1, memIdx)
		require.Contains(t, lines[cpuIdx], dbName0)
		require.Contains(t, lines[cpuIdx], rpName0)
		require.Contains(t, lines[memIdx], dbName1)
		require.Contains(t, lines[memIdx], rpName0)

		require.Less(t, w1, nameHdrIdx, "all warnings precede the table")
		require.Less(t, dashIdx, cpuIdx, "data rows come after the dashed underline")
		require.Less(t, cpuIdx, memIdx, "row order matches series Values order")
	})
}

// TestCommandLine_FormatResponse_JSON_PartialMeasurements is the JSON-format
// companion to TestCommandLine_FormatResponse_Column_PartialMeasurements. It
// verifies that when SHOW MEASUREMENTS returns partial results — some rows
// plus warning Messages produced by partialMeasurementsWarning — the influx
// CLI's "json" formatter emits a single line of valid JSON encoding the full
// client.Response: results[].series with name/columns/values, and
// results[].messages with the level/text pair for each warning, in order.
func TestCommandLine_FormatResponse_JSON_PartialMeasurements(t *testing.T) {
	t.Parallel()

	// wireResponse mirrors what client.Response/Result/Message marshal to,
	// independent of the in-memory structs — so the test pins the on-the-wire
	// JSON shape that downstream consumers parse.
	type wireMessage struct {
		Level string `json:"level"`
		Text  string `json:"text"`
	}
	type wireRow struct {
		Name    string          `json:"name"`
		Columns []string        `json:"columns"`
		Values  [][]interface{} `json:"values"`
	}
	type wireResult struct {
		Series   []wireRow     `json:"series"`
		Messages []wireMessage `json:"messages"`
		Error    string        `json:"error,omitempty"`
	}
	type wireResponse struct {
		Results []wireResult `json:"results"`
		Error   string       `json:"error,omitempty"`
	}

	decode := func(t *testing.T, out string) wireResponse {
		t.Helper()
		// CLI appends a trailing newline via Fprintln; tolerate it.
		require.True(t, strings.HasSuffix(out, "\n"), "writeJSON should terminate with a newline; got %q", out)
		var got wireResponse
		require.NoError(t, json.Unmarshal([]byte(strings.TrimRight(out, "\n")), &got),
			"writeJSON should emit valid JSON; got %q", out)
		return got
	}

	t.Run("single_source_one_warning_compact", func(t *testing.T) {
		t.Parallel()
		resp := &client.Response{
			Results: []client.Result{{
				Messages: []*client.Message{{
					Level: levelWarning,
					Text:  warnTextSingleDB0,
				}},
				Series: []models.Row{{
					Name:    seriesNameMeasurements,
					Columns: []string{colNameField},
					Values:  [][]interface{}{{measurementCPU}, {measurementMem}},
				}},
			}},
		}

		c := cli.New(CLIENT_VERSION)
		c.Format = formatJSON
		var buf bytes.Buffer
		c.FormatResponse(resp, &buf)

		out := buf.String()
		// Compact (non-pretty) JSON: one line of JSON, then a trailing newline.
		require.Equal(t, 0, strings.Count(strings.TrimRight(out, "\n"), "\n"),
			"compact JSON should be a single line; got %q", out)
		require.NotContains(t, out, "    ", "compact JSON should not be indented; got %q", out)

		got := decode(t, out)
		require.Empty(t, got.Error, "no top-level error expected on partial success")
		require.Len(t, got.Results, 1)
		r := got.Results[0]
		require.Empty(t, r.Error, "no per-result error expected on partial success")
		require.Equal(t, []wireMessage{{
			Level: levelWarning,
			Text:  warnTextSingleDB0,
		}}, r.Messages, "JSON message text is the raw coordinator string — no trailing period (unlike the column formatter)")
		require.Equal(t, []wireRow{{
			Name:    seriesNameMeasurements,
			Columns: []string{colNameField},
			Values:  [][]interface{}{{measurementCPU}, {measurementMem}},
		}}, r.Series)
	})

	t.Run("wildcard_two_warnings_three_columns_pretty", func(t *testing.T) {
		t.Parallel()
		resp := &client.Response{
			Results: []client.Result{{
				Messages: []*client.Message{
					{Level: levelWarning, Text: warnTextDB0RP0},
					{Level: levelWarning, Text: warnTextDB1RP0},
				},
				Series: []models.Row{{
					Name:    seriesNameMeasurements,
					Columns: []string{colNameField, colDatabase, colRetentionPolicy},
					Values: [][]interface{}{
						{measurementCPU, dbName0, rpName0},
						{measurementMem, dbName1, rpName0},
					},
				}},
			}},
		}

		c := cli.New(CLIENT_VERSION)
		c.Format = formatJSON
		c.Pretty = true
		var buf bytes.Buffer
		c.FormatResponse(resp, &buf)

		out := buf.String()
		// Pretty JSON: multi-line, indented with four spaces (per writeJSON).
		require.Greater(t, strings.Count(out, "\n"), 1,
			"pretty JSON should span multiple lines; got %q", out)
		require.Contains(t, out, "\n    \"results\":",
			"pretty JSON should indent top-level fields with four spaces")

		got := decode(t, out)
		require.Empty(t, got.Error)
		require.Len(t, got.Results, 1)
		r := got.Results[0]
		require.Empty(t, r.Error)
		require.Equal(t, []wireMessage{
			{Level: levelWarning, Text: warnTextDB0RP0},
			{Level: levelWarning, Text: warnTextDB1RP0},
		}, r.Messages, "messages preserved in order with original (unpunctuated) coordinator text")
		require.Equal(t, []wireRow{{
			Name:    seriesNameMeasurements,
			Columns: []string{colNameField, colDatabase, colRetentionPolicy},
			Values: [][]interface{}{
				{measurementCPU, dbName0, rpName0},
				{measurementMem, dbName1, rpName0},
			},
		}}, r.Series)
	})
}

// helper methods

func emptyTestServer() *httptest.Server {
	return emptyTestServerWithPath("")
}
