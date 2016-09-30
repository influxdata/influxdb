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
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/peterh/liner"
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

func TestRunCLI(t *testing.T) {
	t.Parallel()
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	h, p, _ := net.SplitHostPort(u.Host)
	c := cli.New(CLIENT_VERSION)
	c.Host = h
	c.Port, _ = strconv.Atoi(p)
	c.IgnoreSignals = true
	go func() {
		close(c.Quit)
	}()
	if err := c.Run(); err != nil {
		t.Fatalf("Run failed with error: %s", err)
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
	c.Precision = "ms"
	c.Execute = "INSERT sensor,floor=1 value=2"
	c.IgnoreSignals = true
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
	if c.Username != u {
		t.Fatalf("Username is %s but should be %s", c.Username, u)
	}
	if c.Password != p {
		t.Fatalf("Password is %s but should be %s", c.Password, p)
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
	if c.Precision != p {
		t.Fatalf("Precision is %s but should be %s", c.Precision, p)
	}

	// validate set default precision which equals empty string
	p = "rfc3339"
	c.SetPrecision("precision " + p)
	if c.Precision != "" {
		t.Fatalf("Precision is %s but should be empty", c.Precision)
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
}

func createFakeResponse(rows ...models.Row) *client.Response {
	result1 := client.Result{Series: rows}
	results := []client.Result{result1}
	response := &client.Response{Results: results}
	return response
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func TestLineProtocolBasic(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{Format: "lineprotocol"}

	expects := []string{}
	row1 := models.Row{
		Name:    "cpu",
		Tags:    map[string]string{"host": "anomaly.io"},
		Columns: []string{"time", "value", "latency"},
		Values: [][]interface{}{
			{json.Number("1300000000"), float64(100), 12},
			{json.Number("1400000000"), int64(200), 80},
			{json.Number("1500000001"), int64(222), 333},
			{json.Number("888"), float64(123), float64(456)},
		},
	}
	expects = append(expects, "cpu,host=anomaly.io latency=12i,value=100 1300000000")
	expects = append(expects, "cpu,host=anomaly.io latency=80i,value=200i 1400000000")
	expects = append(expects, "cpu,host=anomaly.io latency=333i,value=222i 1500000001")
	expects = append(expects, "cpu,host=anomaly.io latency=456,value=123 888")

	row2 := models.Row{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "detection"},
		Values: [][]interface{}{
			{json.Number("1500000000"), true},
			{json.Number("1600000000"), false},
		},
	}
	expects = append(expects, "cpu detection=true 1500000000")
	expects = append(expects, "cpu detection=false 1600000000")

	output := new(bytes.Buffer)
	c.FormatResponse(createFakeResponse(row1, row2), output)

	lpRows := strings.Split(output.String(), "\n")
	lpRows = lpRows[0 : len(lpRows)-1] //remove last /n

	checkMatch(t, lpRows, expects)
}

// Tag are map[string]. The order is guaranteed by sorting the tag.
// Test tag should be in order: "a_instancetype" then "b_domain"
func TestLineProtocolMultiTag(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{Format: "lineprotocol"}

	expects := []string{}
	row1 := models.Row{
		Name: "cpu",
		Tags: map[string]string{"b_domain": "anomaly.io",
			"a_instancetype": "big"},
		Columns: []string{"time", "value"},
		Values: [][]interface{}{
			{json.Number("1300000000"), float64(100)},
			{json.Number("1400000000"), float64(123)},
		},
	}
	expects = append(expects, "cpu,a_instancetype=big,b_domain=anomaly.io value=100 1300000000")
	expects = append(expects, "cpu,a_instancetype=big,b_domain=anomaly.io value=123 1400000000")

	output := new(bytes.Buffer)
	c.FormatResponse(createFakeResponse(row1), output)

	lpRows := strings.Split(output.String(), "\n")
	lpRows = lpRows[0 : len(lpRows)-1] //remove last line as it's an empty line

	checkMatch(t, lpRows, expects)
}

func TestLineProtocolSpecialChar(t *testing.T) {
	t.Parallel()
	c := cli.CommandLine{Format: "lineprotocol"}

	expects := []string{}
	row1 := models.Row{
		Name:    "my strange=long,measurement",
		Tags:    map[string]string{"my strange=long,tag key": "my strange=long,tag value"},
		Columns: []string{"time", "value", "my strange=long,column name"},
		Values: [][]interface{}{
			{json.Number("1300000000"), float64(100), "my strange=long,column value with \" <-double-quote!  "},
		},
	}
	var buffer bytes.Buffer
	buffer.WriteString("my\\ strange=long\\,measurement") // escape: comma and space
	buffer.WriteString(",")
	buffer.WriteString("my\\ strange\\=long\\,tag\\ key") //escape: commas, spaces, and equal signs
	buffer.WriteString("=")
	buffer.WriteString("my\\ strange\\=long\\,tag\\ value") //escape: commas, spaces, and equal signs
	buffer.WriteString(" ")
	buffer.WriteString("my\\ strange\\=long\\,column\\ name")
	buffer.WriteString("=")
	buffer.WriteString("\"my strange=long,column value with \\\" <-double-quote!  \"") //escape: double-quote
	buffer.WriteString(",value=100")

	buffer.WriteString(" 1300000000")
	expects = append(expects, buffer.String())

	row2 := models.Row{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "notstrange:-)àç!èè§(''\"é&", "str_field"},
		Values: [][]interface{}{
			{json.Number("1500000000"), true, "hello world"},
			{json.Number("1600000000"), false, "martin magakian"},
		},
	}
	expects = append(expects, "cpu notstrange:-)àç!èè§(''\\\"é&=true,str_field=\"hello world\" 1500000000")
	expects = append(expects, "cpu notstrange:-)àç!èè§(''\\\"é&=false,str_field=\"martin magakian\" 1600000000")

	output := new(bytes.Buffer)
	c.FormatResponse(createFakeResponse(row1, row2), output)

	lpRows := strings.Split(output.String(), "\n")
	lpRows = lpRows[0 : len(lpRows)-1] //remove last line as it's an empty line

	checkMatch(t, lpRows, expects)
}

func checkMatch(t *testing.T, lpRows []string, expects []string) {
	for _, line := range lpRows {
		if !contains(expects, line) {
			for _, c := range expects {
				fmt.Println(c)
			}
			t.Fatalf("LineProtocol:\n%s\nwas not expected", line)
		}
	}

	if len(lpRows) != len(expects) {
		t.Fatalf("expected %d lineProtocol but get %d lineProtocol", len(expects), len(lpRows))
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
	if c.WriteConsistency != consistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.WriteConsistency, consistency)
	}

	// set different valid write consistency and validate change
	consistency = "quorum"
	c.SetWriteConsistency("consistency " + consistency)
	if c.WriteConsistency != consistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.WriteConsistency, consistency)
	}

	// set invalid write consistency and verify there was no change
	invalidConsistency := "invalid_consistency"
	c.SetWriteConsistency("consistency " + invalidConsistency)
	if c.WriteConsistency == invalidConsistency {
		t.Fatalf("WriteConsistency is %s but should be %s", c.WriteConsistency, consistency)
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
	}{
		{cmd: "use db"},
		{cmd: " use db"},
		{cmd: "use db "},
		{cmd: "use db;"},
		{cmd: "use db; "},
		{cmd: "Use db"},
	}

	for _, test := range tests {
		m := cli.CommandLine{Client: c}
		if err := m.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}

		if m.Database != "db" {
			t.Fatalf(`Command "use" changed database to %q. Expected db`, m.Database)
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
		m := cli.CommandLine{Client: c, Username: tt.user}

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

		if c.WriteConsistency != "one" {
			t.Fatalf(`Command "consistency" changed consistency to %q. Expected one`, c.WriteConsistency)
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

func TestParseCommand_InsertInto(t *testing.T) {
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
		cmd, db, rp string
	}{
		{
			cmd: `INSERT INTO test cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test",
		},
		{
			cmd: ` INSERT INTO .test cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test",
		},
		{
			cmd: `INSERT INTO   "test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "",
			rp:  "test test",
		},
		{
			cmd: `Insert iNTO test.test cpu,host=serverA,region=us-west value=1.0`,
			db:  "test",
			rp:  "test",
		},
		{
			cmd: `insert into "test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "test",
			rp:  "test test",
		},
		{
			cmd: `insert into "d b"."test test" cpu,host=serverA,region=us-west value=1.0`,
			db:  "d b",
			rp:  "test test",
		},
	}

	for _, test := range tests {
		if err := m.ParseCommand(test.cmd); err != nil {
			t.Fatalf(`Got error %v for command %q, expected nil.`, err, test.cmd)
		}
		if m.Database != test.db {
			t.Fatalf(`Command "insert into" db parsing failed, expected: %q, actual: %q`, test.db, m.Database)
		}
		if m.RetentionPolicy != test.rp {
			t.Fatalf(`Command "insert into" rp parsing failed, expected: %q, actual: %q`, test.rp, m.RetentionPolicy)
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

// helper methods

func emptyTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Influxdb-Version", SERVER_VERSION)

		// Fake authorization entirely based on the username.
		authorized := false
		user, _, _ := r.BasicAuth()
		switch user {
		case "", "admin":
			authorized = true
		}

		switch r.URL.Path {
		case "/query":
			values := r.URL.Query()
			parser := influxql.NewParser(bytes.NewBufferString(values.Get("q")))
			q, err := parser.ParseQuery()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			stmt := q.Statements[0]

			switch stmt.(type) {
			case *influxql.ShowDatabasesStatement:
				if authorized {
					io.WriteString(w, `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db"]]}]}]}`)
				} else {
					w.WriteHeader(http.StatusUnauthorized)
					io.WriteString(w, fmt.Sprintf(`{"error":"error authorizing query: %s not authorized to execute statement 'SHOW DATABASES', requires admin privilege"}`, user))
				}
			case *influxql.ShowDiagnosticsStatement:
				io.WriteString(w, `{"results":[{}]}`)
			}
		case "/write":
			w.WriteHeader(http.StatusOK)
		}
	}))
}
