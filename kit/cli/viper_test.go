package cli

import (
	"fmt"
	"os"
)

func ExampleNewCommand() {
	var monitorHost string
	var number int
	cmd := NewCommand(&Program{
		Run: func() error {
			fmt.Println(monitorHost)
			for i := 0; i < number; i++ {
				fmt.Printf("%d\n", i)
			}
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
		},
	})

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	// Output:
	// http://localhost:8086
	// 0
	// 1
}
