package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/influxdata/platform/http"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var transpileCmd = &cobra.Command{
	Use:   "transpilerd",
	Short: "Transpiler Query Server",
	Run:   transpileF,
}

// Flags contains all the CLI flag values for transpilerd.
type Flags struct {
	bindAddr string
}

var flags Flags

func init() {
	viper.SetEnvPrefix("TRANSPILERD")

	transpileCmd.PersistentFlags().StringVar(&flags.bindAddr, "bind-addr", ":8098", "The bind address for this daemon.")
	viper.BindEnv("BIND_ADDR")
	if b := viper.GetString("BIND_ADDR"); b != "" {
		flags.bindAddr = b
	}

	// TODO(jsternberg): Connect directly to the storage hosts. There's no need to require proxying
	// the requests through ifqld for this service.
	transpileCmd.PersistentFlags().String("ifqld-hosts", "http://localhost:8093", "scheme://host:port address of the ifqld server.")
	viper.BindEnv("IFQLD_HOSTS")
	viper.BindPFlag("IFQLD_HOSTS", transpileCmd.PersistentFlags().Lookup("ifqld-hosts"))
}

func transpileF(cmd *cobra.Command, args []string) {
	hosts, err := discoverHosts()
	if err != nil {
		log.Fatal(err)
	} else if len(hosts) == 0 {
		log.Fatal("no ifqld hosts found")
	}
	transpileHandler := http.NewTranspilerQueryHandler()
	transpileHandler.QueryService = &http.QueryService{
		//TODO(nathanielc): Allow QueryService to use multiple hosts.
		Addr: hosts[0],
	}

	//TODO(nathanielc): Add health checks

	handler := http.NewHandler("transpile")
	handler.Handler = transpileHandler

	log.Printf("Starting transpilerd on %s\n", flags.bindAddr)
	if err := http.ListenAndServe(flags.bindAddr, handler, nil); err != nil {
		log.Fatal(err)
	}
}

func main() {
	if err := transpileCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getStrList(key string) ([]string, error) {
	v := viper.GetViper()
	valStr := v.GetString(key)
	if valStr == "" {
		return nil, errors.New("empty value")
	}

	return strings.Split(valStr, ","), nil
}

func discoverHosts() ([]string, error) {
	ifqldHosts, err := getStrList("IFQLD_HOSTS")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ifqld hosts")
	}
	return ifqldHosts, nil
}
