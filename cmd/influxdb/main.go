package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/influxdb/influxdb/client"
)

type cli struct {
	client   *client.Client
	host     string
	port     int
	username string
	password string
	database string
	version  string
	pretty   bool // controls pretty print for json
}

func main() {
	c := cli{}

	fs := flag.NewFlagSet("default", flag.ExitOnError)
	fs.StringVar(&c.host, "host", "localhost", "influxdb host to connect to")
	fs.IntVar(&c.port, "port", 8086, "influxdb port to connect to")
	fs.StringVar(&c.username, "username", c.username, "username to connect to the server.  can be blank if authorization is not required")
	fs.StringVar(&c.password, "password", c.password, "password to connect to the server.  can be blank if authorization is not required")
	fs.StringVar(&c.database, "database", c.database, "database to connect to the server.")
	fs.Parse(os.Args[1:])

	// TODO Determine if we are an ineractive shell or running commands
	fmt.Println("InfluxDB shell")
	c.connect("")
	fmt.Print("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		c.parseCommand(scanner.Text())
		fmt.Print("> ")
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

func (c *cli) connect(cmd string) {
	var cl *client.Client
	if cmd != "" {
		// TODO parse out connection string
	}
	u := url.URL{
		Scheme: "http",
	}
	if c.port > 0 {
		u.Host = fmt.Sprintf("%s:%d", c.host, c.port)
	} else {
		u.Host = c.host
	}
	if c.username != "" {
		u.User = url.UserPassword(c.username, c.password)
	}
	cl, err := client.NewClient(
		client.Config{
			URL:      u,
			Username: c.username,
			Password: c.password,
		})
	if err != nil {
		fmt.Printf("Could not create client %s", err)
		return
	}
	c.client = cl
	if _, v, e := c.client.Ping(); e != nil {
		fmt.Printf("Failed to connect to %s\n", c.client.Addr())
	} else {
		c.version = v
		fmt.Printf("Connected to %s version %s\n", c.client.Addr(), c.version)
	}
}

func (c *cli) parseCommand(cmd string) {
	lcmd := strings.ToLower(cmd)
	switch {
	case strings.HasPrefix(lcmd, "exit"):
		os.Exit(0)
	case strings.HasPrefix(lcmd, "gopher"):
		gopher()
	case strings.HasPrefix(lcmd, "connect"):
		c.connect(cmd)
	case strings.HasPrefix(lcmd, "pretty"):
		c.pretty = !c.pretty
		if c.pretty {
			fmt.Println("Pretty print enabled")
		} else {
			fmt.Println("Pretty print disabled")
		}
	case strings.HasPrefix(lcmd, "use"):
		fmt.Println("We should use a database specified now... todo")
	default:
		c.executeQuery(cmd)
	}
}

func (c *cli) executeQuery(query string) {
	results, err := c.client.Query(client.Query{Command: query, Database: c.database})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
	}
	for _, r := range results {
		var i interface{}
		if r.Err != nil {
			i = r.Err
		} else {
			if len(r.Rows) == 0 {
				continue
			}
			i = r.Rows
		}
		var data []byte
		if c.pretty {
			data, err = json.MarshalIndent(i, "", "    ")
		} else {
			data, err = json.Marshal(i)
		}
		if err != nil {
			fmt.Printf("ERR: %s\n", err)
			return
		}
		fmt.Fprintln(os.Stdout, string(data))
	}
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
