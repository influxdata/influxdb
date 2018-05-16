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

var idpCmd = &cobra.Command{
	Use:   "idp",
	Short: "IDP Client",
	Run:   idpF,
}

func init() {
	idpCmd.AddCommand(authorizationCmd)
	idpCmd.AddCommand(bucketCmd)
	idpCmd.AddCommand(ifqlCmd)
	idpCmd.AddCommand(organizationCmd)
	idpCmd.AddCommand(userCmd)
}

// Flags contains all the CLI flag values for idp.
type Flags struct {
	token string
	host  string
}

var flags Flags

func init() {
	viper.SetEnvPrefix("IDP")

	idpCmd.PersistentFlags().StringVarP(&flags.token, "token", "t", "", "API token to be used throughout client calls")
	viper.BindEnv("TOKEN")
	if h := viper.GetString("TOKEN"); h != "" {
		flags.token = h
	}

	idpCmd.PersistentFlags().StringVar(&flags.host, "host", "http://localhost:9999", "HTTP address of IDP")
	viper.BindEnv("HOST")
	if h := viper.GetString("HOST"); h != "" {
		flags.host = h
	}
}

func idpF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Execute executes the idpd command
func Execute() {
	if err := idpCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
