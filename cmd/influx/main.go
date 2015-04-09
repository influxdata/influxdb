package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/influxdb/influxdb/client"
	"github.com/peterh/liner"
)

// These variables are populated via the Go linker.
var (
	version string = "0.9"
)

const (
	default_host   = "localhost"
	default_port   = 8086
	default_format = "column"
)

type CommandLine struct {
	Client     *client.Client
	Line       *liner.State
	Host       string
	Port       int
	Username   string
	Password   string
	Database   string
	Version    string
	Pretty     bool   // controls pretty print for json
	Format     string // controls the output format.  Valid values are json, csv, or column
	ShouldDump bool
	Execute    string
}

func main() {
	c := CommandLine{}

	fs := flag.NewFlagSet("default", flag.ExitOnError)
	fs.StringVar(&c.Host, "host", default_host, "influxdb host to connect to")
	fs.IntVar(&c.Port, "port", default_port, "influxdb port to connect to")
	fs.StringVar(&c.Username, "username", c.Username, "username to connect to the server.")
	fs.StringVar(&c.Password, "password", c.Password, `password to connect to the server.  Leaving blank will prompt for password (--password="")`)
	fs.StringVar(&c.Database, "database", c.Database, "database to connect to the server.")
	fs.StringVar(&c.Format, "output", default_format, "format specifies the format of the server responses:  json, csv, or column")
	fs.BoolVar(&c.ShouldDump, "dump", false, "dump the contents of the given database to stdout")
	fs.StringVar(&c.Execute, "execute", c.Execute, "Execute command and quit.")
	fs.Parse(os.Args[1:])

	var promptForPassword bool
	// determine if they set the password flag but provided no value
	for _, v := range os.Args {
		v = strings.ToLower(v)
		if (strings.HasPrefix(v, "-password") || strings.HasPrefix(v, "--password")) && c.Password == "" {
			promptForPassword = true
			break
		}
	}

	c.Line = liner.NewLiner()
	defer c.Line.Close()

	if promptForPassword {
		p, e := c.Line.PasswordPrompt("password: ")
		if e != nil {
			fmt.Println("Unable to parse password.")
		} else {
			c.Password = p
		}
	}

	c.connect("")

	if c.ShouldDump {
		c.dump()
		return
	}

	if c.Execute != "" {
		c.executeQuery(c.Execute)
		return
	}

	fmt.Println("InfluxDB shell " + version)

	var historyFile string
	usr, err := user.Current()
	// Only load history if we can get the user
	if err == nil {
		historyFile = filepath.Join(usr.HomeDir, ".influx_history")

		if f, err := os.Open(historyFile); err == nil {
			c.Line.ReadHistory(f)
			f.Close()
		}
	}

	for {
		l, e := c.Line.Prompt("> ")
		if e != nil {
			break
		}
		if c.ParseCommand(l) {
			// write out the history
			if len(historyFile) > 0 {
				c.Line.AppendHistory(l)
				if f, err := os.Create(historyFile); err == nil {
					c.Line.WriteHistory(f)
					f.Close()
				}
			}
		} else {
			break // exit main loop
		}
	}
}

func (c *CommandLine) ParseCommand(cmd string) bool {
	lcmd := strings.TrimSpace(strings.ToLower(cmd))
	switch {
	case strings.HasPrefix(lcmd, "exit"):
		// signal the program to exit
		return false
	case strings.HasPrefix(lcmd, "gopher"):
		gopher()
	case strings.HasPrefix(lcmd, "connect"):
		c.connect(cmd)
	case strings.HasPrefix(lcmd, "auth"):
		c.SetAuth()
	case strings.HasPrefix(lcmd, "help"):
		help()
	case strings.HasPrefix(lcmd, "format"):
		c.SetFormat(cmd)
	case strings.HasPrefix(lcmd, "settings"):
		c.Settings()
	case strings.HasPrefix(lcmd, "pretty"):
		c.Pretty = !c.Pretty
		if c.Pretty {
			fmt.Println("Pretty print enabled")
		} else {
			fmt.Println("Pretty print disabled")
		}
	case strings.HasPrefix(lcmd, "use"):
		c.use(cmd)
	case lcmd == "":
		break
	default:
		c.executeQuery(cmd)
	}
	return true
}

func (c *CommandLine) connect(cmd string) {
	var cl *client.Client

	if cmd != "" {
		// Remove the "connect" keyword if it exists
		cmd = strings.TrimSpace(strings.Replace(cmd, "connect", "", -1))
		if cmd == "" {
			return
		}
		if strings.Contains(cmd, ":") {
			h := strings.Split(cmd, ":")
			if i, e := strconv.Atoi(h[1]); e != nil {
				fmt.Printf("Connect error: Invalid port number %q: %s\n", cmd, e)
				return
			} else {
				c.Port = i
			}
			if h[0] == "" {
				c.Host = default_host
			} else {
				c.Host = h[0]
			}
		} else {
			c.Host = cmd
			// If they didn't specify a port, always use the default port
			c.Port = default_port
		}
	}

	u := url.URL{
		Scheme: "http",
	}
	if c.Port > 0 {
		u.Host = fmt.Sprintf("%s:%d", c.Host, c.Port)
	} else {
		u.Host = c.Host
	}
	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}
	cl, err := client.NewClient(
		client.Config{
			URL:       u,
			Username:  c.Username,
			Password:  c.Password,
			UserAgent: "InfluxDBShell/" + version,
		})
	if err != nil {
		fmt.Printf("Could not create client %s", err)
		return
	}
	c.Client = cl
	if _, v, e := c.Client.Ping(); e != nil {
		fmt.Printf("Failed to connect to %s\n", c.Client.Addr())
	} else {
		c.Version = v
		if !c.ShouldDump && c.Execute == "" {
			fmt.Printf("Connected to %s version %s\n", c.Client.Addr(), c.Version)
		}
	}
}

func (c *CommandLine) SetAuth() {
	u, e := c.Line.Prompt("username: ")
	if e != nil {
		fmt.Printf("Unable to process input: %s", e)
		return
	}
	c.Username = strings.TrimSpace(u)
	p, e := c.Line.PasswordPrompt("password: ")
	if e != nil {
		fmt.Printf("Unable to process input: %s", e)
		return
	}
	c.Password = p
}

func (c *CommandLine) use(cmd string) {
	args := strings.Split(strings.TrimSpace(cmd), " ")
	if len(args) != 2 {
		fmt.Printf("Could not parse database name from %q.\n", cmd)
		return
	}
	d := args[1]
	c.Database = d
	fmt.Printf("Using database %s\n", d)
}

func (c *CommandLine) SetFormat(cmd string) {
	// Remove the "format" keyword if it exists
	cmd = strings.TrimSpace(strings.Replace(cmd, "format", "", -1))
	// normalize cmd
	cmd = strings.ToLower(cmd)

	switch cmd {
	case "json", "csv", "column":
		c.Format = cmd
	default:
		fmt.Printf("Unknown format %q. Please use json, csv, or column.\n", cmd)
	}
}

func (c *CommandLine) dump() {
	response, err := c.Client.Dump(c.Database)
	defer response.Close()
	if err != nil {
		fmt.Printf("Dump failed. %s\n", err)
	} else {
		_, err := io.Copy(os.Stdout, response)
		if err != nil {
			fmt.Printf("Dump failed. %s\n", err)
		}
	}
}

func (c *CommandLine) executeQuery(query string) {
	response, err := c.Client.Query(client.Query{Command: query, Database: c.Database})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return
	}
	c.FormatResponse(response, os.Stdout)
	if response.Error() != nil {
		fmt.Printf("ERR: %s\n", response.Error())
		if c.Database == "" {
			fmt.Println("Warning: It is possible this error is due to not setting a database.")
			fmt.Println(`Please set a database with the command "use <database>".`)
		}
	}
}

func (c *CommandLine) FormatResponse(response *client.Response, w io.Writer) {
	switch c.Format {
	case "json":
		c.writeJSON(response, w)
	case "csv":
		c.writeCSV(response, w)
	case "column":
		c.writeColumns(response, w)
	default:
		fmt.Fprintf(w, "Unknown output format %q.\n", c.Format)
	}
}

func (c *CommandLine) writeJSON(response *client.Response, w io.Writer) {
	var data []byte
	var err error
	if c.Pretty {
		data, err = json.MarshalIndent(response, "", "    ")
	} else {
		data, err = json.Marshal(response)
	}
	if err != nil {
		fmt.Fprintf(w, "Unable to parse json: %s\n", err)
		return
	}
	fmt.Fprintln(w, string(data))
}

func (c *CommandLine) writeCSV(response *client.Response, w io.Writer) {
	csvw := csv.NewWriter(w)
	for _, result := range response.Results {
		// Create a tabbed writer for each result as they won't always line up
		rows := c.formatResults(result, "\t")
		for _, r := range rows {
			csvw.Write(strings.Split(r, "\t"))
		}
		csvw.Flush()
	}
}

func (c *CommandLine) writeColumns(response *client.Response, w io.Writer) {
	for _, result := range response.Results {
		// Create a tabbed writer for each result a they won't always line up
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 8, 1, '\t', 0)
		csv := c.formatResults(result, "\t")
		for _, r := range csv {
			fmt.Fprintln(w, r)
		}
		w.Flush()
	}
}

// formatResults will behave differently if you are formatting for columns or csv
func (c *CommandLine) formatResults(result client.Result, separator string) []string {
	rows := []string{}
	// Create a tabbed writer for each result a they won't always line up
	for i, row := range result.Series {
		// gather tags
		tags := []string{}
		for k, v := range row.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
			sort.Strings(tags)
		}

		columnNames := []string{}

		// Only put name/tags in a column if format is csv
		if c.Format == "csv" {
			if len(tags) > 0 {
				columnNames = append([]string{"tags"}, columnNames...)
			}

			if row.Name != "" {
				columnNames = append([]string{"name"}, columnNames...)
			}
		}

		for _, column := range row.Columns {
			columnNames = append(columnNames, column)
		}

		// Output a line separator if we have more than one set or results and format is column
		if i > 0 && c.Format == "column" {
			rows = append(rows, "")
		}

		// If we are column format, we break out the name/tag to seperate lines
		if c.Format == "column" {
			if row.Name != "" {
				n := fmt.Sprintf("name: %s", row.Name)
				rows = append(rows, n)
				if len(tags) == 0 {
					l := strings.Repeat("-", len(n))
					rows = append(rows, l)
				}
			}
			if len(tags) > 0 {
				t := fmt.Sprintf("tags: %s", (strings.Join(tags, ", ")))
				rows = append(rows, t)
			}
		}

		rows = append(rows, strings.Join(columnNames, separator))

		// if format is column, break tags to their own line/format
		if c.Format == "column" && len(tags) > 0 {
			lines := []string{}
			for _, columnName := range columnNames {
				lines = append(lines, strings.Repeat("-", len(columnName)))
			}
			rows = append(rows, strings.Join(lines, separator))
		}

		for _, v := range row.Values {
			var values []string
			if c.Format == "csv" {
				if row.Name != "" {
					values = append(values, row.Name)
				}
				if len(tags) > 0 {
					values = append(values, strings.Join(tags, ","))
				}
			}

			for _, vv := range v {
				values = append(values, interfaceToString(vv))
			}
			rows = append(rows, strings.Join(values, separator))
		}
		// Outout a line separator if in column format
		if c.Format == "column" {
			rows = append(rows, "")
		}
	}
	return rows
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func (c *CommandLine) Settings() {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 1, '\t', 0)
	if c.Port > 0 {
		fmt.Fprintf(w, "Host\t%s:%d\n", c.Host, c.Port)
	} else {
		fmt.Fprintf(w, "Host\t%s\n", c.Host)
	}
	fmt.Fprintf(w, "Username\t%s\n", c.Username)
	fmt.Fprintf(w, "Database\t%s\n", c.Database)
	fmt.Fprintf(w, "Pretty\t%v\n", c.Pretty)
	fmt.Fprintf(w, "Format\t%s\n", c.Format)
	fmt.Fprintln(w)
	w.Flush()
}

func help() {
	fmt.Println(`Usage:
        connect <host:port>   connect to another node
        auth                  prompt for username and password
        pretty                toggle pretty print
        use <db_name>         set current databases
        format <format>       set the output format: json, csv, or column
        settings              output the current settings for the shell
        exit                  quit the influx shell

        show databases        show database names
        show series           show series information
        show measurements     show measurement information
        show tag keys         show tag key information
        show tag values       show tag value information

        a full list of influxql commands can be found at:
        http://influxdb.com/docs
`)
}

func gopher() {
	fmt.Println(`
                                          .-::-::://:-::-    .:/++/'
                                     '://:-''/oo+//++o+/.://o-    ./+:
                                  .:-.    '++-         .o/ '+yydhy'  o-
                               .:/.      .h:         :osoys  .smMN-  :/
                            -/:.'        s-         /MMMymh.   '/y/  s'
                         -+s:''''        d          -mMMms//     '-/o:
                       -/++/++/////:.    o:          '... s-        :s.
                     :+-+s-'       ':/'  's-             /+          'o:
                   '+-'o:        /ydhsh.  '//.        '-o-             o-
                  .y. o:        .MMMdm+y    ':+++:::/+:.'               s:
                .-h/  y-        'sdmds'h -+ydds:::-.'                   'h.
             .//-.d'  o:          '.' 'dsNMMMNh:.:++'                    :y
            +y.  'd   's.            .s:mddds:     ++                     o/
           'N-  odd    'o/.       './o-s-'   .---+++'                      o-
           'N'  yNd      .://:/:::::. -s   -+/s/./s'                       'o/'
            so'  .h         ''''       ////s: '+. .s                         +y'
             os/-.y'                       's' 'y::+                          +d'
               '.:o/                        -+:-:.'                            so.---.'
                   o'                                                          'd-.''/s'
                   .s'                                                          :y.''.y
                    -s                                                           mo:::'
                     ::                                                          yh
                      //                                      ''''               /M'
                       o+                                    .s///:/.            'N:
                        :+                                   /:    -s'            ho
                         's-                               -/s/:+/.+h'            +h
                           ys'                            ':'    '-.              -d
                            oh                                                    .h
                             /o                                                   .s
                              s.                                                  .h
                              -y                                                  .d
                               m/                                                 -h
                               +d                                                 /o
                               'N-                                                y:
                                h:                                                m.
                                s-                                               -d
                                o-                                               s+
                                +-                                              'm'
                                s/                                              oo--.
                                y-                                             /s  ':+'
                                s'                                           'od--' .d:
                                -+                                         ':o: ':+-/+
                                 y-                                      .:+-      '
                                //o-                                 '.:+/.
                                .-:+/'                           ''-/+/.
                                    ./:'                    ''.:o+/-'
                                      .+o:/:/+-'      ''.-+ooo/-'
                                         o:   -h///++////-.
                                        /:   .o/
                                       //+  'y
                                       ./sooy.

`)
}
