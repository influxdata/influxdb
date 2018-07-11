package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/kit/prom"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var transpileCmd = &cobra.Command{
	Use:   "transpilerd",
	Short: "Transpiler Query Server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := influxlogger.New(os.Stdout)
		if err := transpileF(cmd, logger, args); err != nil && err != context.Canceled {
			logger.Error("Encountered fatal error", zap.String("error", err.Error()))
			os.Exit(1)
		}
	},
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
	// the requests through fluxd for this service.
	transpileCmd.PersistentFlags().String("fluxd-hosts", "http://localhost:8093", "scheme://host:port address of the fluxd server.")
	viper.BindEnv("FLUXD_HOSTS")
	viper.BindPFlag("FLUXD_HOSTS", transpileCmd.PersistentFlags().Lookup("fluxd-hosts"))

	// TODO(jsternberg): Determine how we are going to identify the organization id in open source.
	transpileCmd.PersistentFlags().StringP("org-id", "", "0000000000000000", "id of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	viper.BindPFlag("ORG_ID", transpileCmd.PersistentFlags().Lookup("org-id"))
}

func transpileF(cmd *cobra.Command, logger *zap.Logger, args []string) error {
	hosts, err := discoverHosts()
	if err != nil {
		return err
	} else if len(hosts) == 0 {
		return errors.New("no fluxd hosts found")
	}

	// Retrieve the organization that we are using.
	id, err := getOrganization()
	if err != nil {
		return err
	}

	reg := prom.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.WithLogger(logger)

	// TODO(nathanielc): Allow QueryService to use multiple hosts.

	logger.Info("Using fluxd service", zap.Strings("hosts", hosts), zap.Stringer("org-id", id))
	transpileHandler := http.NewTranspilerQueryHandler(id)
	transpileHandler.QueryService = &http.QueryService{
		Addr: hosts[0],
	}
	transpileHandler.Logger = logger
	reg.MustRegister(transpileHandler.PrometheusCollectors()...)

	//TODO(nathanielc): Add health checks

	handler := http.NewHandlerFromRegistry("transpile", reg)
	handler.Handler = transpileHandler

	logger.Info("Starting transpilerd", zap.String("bind_addr", flags.bindAddr))
	return http.ListenAndServe(flags.bindAddr, handler, logger)
}

func main() {
	if err := transpileCmd.Execute(); err != nil {
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

func getOrganization() (platform.ID, error) {
	v := viper.GetViper()
	orgID := v.GetString("ORG_ID")
	if orgID == "" {
		return nil, errors.New("must specify org-id")
	}

	var id platform.ID
	if err := id.DecodeFromString(orgID); err != nil {
		return nil, fmt.Errorf("unable to decode organization id: %s", err)
	}
	return id, nil
}

func discoverHosts() ([]string, error) {
	fluxdHosts, err := getStrList("FLUXD_HOSTS")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get fluxd hosts")
	}
	return fluxdHosts, nil
}
