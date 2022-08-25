// Package cli creates simple CLI options with ENV overrides using viper.
//
// This is a small simplification over viper to move most of the boilerplate
// into one place.
//
// In this example the flags can be set with MYPROGRAM_MONITOR_HOST and
// MYPROGRAM_NUMBER or with the flags --monitor-host and --number
//
//	var flags struct {
//		monitorHost string
//		number int
//	}
//
//	func main() {
//		cmd := cli.NewCommand(&cli.Program{
//			Run:  run,
//			Name: "myprogram",
//			Opts: []cli.Opt{
//				{
//					DestP:   &flags.monitorHost,
//					Flag:    "monitor-host",
//					Default: "http://localhost:8086",
//					Desc:    "host to send influxdb metrics",
//				},
//				{
//				 	DestP:   &flags.number,
//					Flag:    "number",
//					Default: 2,
//					Desc:    "number of times to loop",
//
//				},
//			},
//		})
//
//		if err := cmd.Execute(); err != nil {
//			fmt.Fprintln(os.Stderr, err)
//			os.Exit(1)
//		}
//	}
//
//	func run() error {
//		for i := 0; i < number; i++ {
//			fmt.Printf("%d\n", i)
//			feturn nil
//		}
//	}
package cli
