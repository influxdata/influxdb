package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	Execute()
}

var influxCmd = &cobra.Command{
	Use:   "influx",
	Short: "Influx Client",
	Run:   influxF,
}

func init() {
	influxCmd.AddCommand(authorizationCmd)
	influxCmd.AddCommand(bucketCmd)
	influxCmd.AddCommand(replCmd)
	influxCmd.AddCommand(queryCmd)
	influxCmd.AddCommand(organizationCmd)
	influxCmd.AddCommand(userCmd)
	influxCmd.AddCommand(setupCmd)
}

// Flags contains all the CLI flag values for influx.
type Flags struct {
	token string
	host  string
}

var flags Flags

func init() {
	viper.SetEnvPrefix("INFLUX")

	influxCmd.PersistentFlags().StringVarP(&flags.token, "token", "t", "", "API token to be used throughout client calls")
	viper.BindEnv("TOKEN")
	if h := viper.GetString("TOKEN"); h != "" {
		flags.token = h
	}

	influxCmd.PersistentFlags().StringVar(&flags.host, "host", "http://localhost:9999", "HTTP address of Influx")
	viper.BindEnv("HOST")
	if h := viper.GetString("HOST"); h != "" {
		flags.host = h
	}
}

func influxF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Execute executes the influx command
func Execute() {
	if err := influxCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
