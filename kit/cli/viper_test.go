package cli

import (
	"fmt"
	"os"
	"time"
)

type customFlag bool

func (c customFlag) String() string {
	if c == true {
		return "on"
	}
	return "off"
}

func (c *customFlag) Set(s string) error {
	if s == "on" {
		*c = true
	} else {
		*c = false
	}

	return nil
}

func (c *customFlag) Type() string {
	return "fancy-bool"
}

func ExampleNewCommand() {
	var monitorHost string
	var number int
	var sleep bool
	var duration time.Duration
	var stringSlice []string
	var fancyBool customFlag
	cmd := NewCommand(&Program{
		Run: func() error {
			fmt.Println(monitorHost)
			for i := 0; i < number; i++ {
				fmt.Printf("%d\n", i)
			}
			fmt.Println(sleep)
			fmt.Println(duration)
			fmt.Println(stringSlice)
			fmt.Println(fancyBool)
			return nil
		},
		Name: "myprogram",
		Opts: []Opt{
			{
				DestP:   &monitorHost,
				Flag:    "monitor-host",
				Default: "http://localhost:8086",
				Desc:    "host to send influxdb metrics",
			},
			{
				DestP:   &number,
				Flag:    "number",
				Default: 2,
				Desc:    "number of times to loop",
			},
			{
				DestP:   &sleep,
				Flag:    "sleep",
				Default: true,
				Desc:    "whether to sleep",
			},
			{
				DestP:   &duration,
				Flag:    "duration",
				Default: time.Minute,
				Desc:    "how long to sleep",
			},
			{
				DestP:   &stringSlice,
				Flag:    "string-slice",
				Default: []string{"foo", "bar"},
				Desc:    "things come in lists",
			},
			{
				DestP:   &fancyBool,
				Flag:    "fancy-bool",
				Default: "on",
				Desc:    "things that implement pflag.Value",
			},
		},
	})

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	// Output:
	// http://localhost:8086
	// 0
	// 1
	// true
	// 1m0s
	// [foo bar]
	// on
}
