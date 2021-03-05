package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/influxdata/influxdb/v2/kit/cli"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/prometheus"
	"github.com/influxdata/influxdb/v2/telemetry"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	addr string
)

func main() {
	logconf := influxlogger.NewConfig()
	log, err := logconf.New(os.Stdout)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to configure logger: %v", err)
		os.Exit(1)
	}

	prog := &cli.Program{
		Run: func() error {
			return run(log)
		},
		Name: "telemetryd",
		Opts: []cli.Opt{
			{
				DestP:   &addr,
				Flag:    "bind-addr",
				Default: ":8080",
				Desc:    "binding address for telemetry server",
			},
		},
	}

	v := viper.New()
	cmd, err := cli.NewCommand(v, prog)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	var exitCode int
	if err := cmd.Execute(); err != nil {
		exitCode = 1
		log.Error("Command returned error", zap.Error(err))
	}

	if err := log.Sync(); err != nil {
		exitCode = 1
		fmt.Fprintf(os.Stderr, "Error syncing logs: %v\n", err)
	}
	time.Sleep(10 * time.Millisecond)
	os.Exit(exitCode)
}

func run(log *zap.Logger) error {
	log = log.With(zap.String("service", "telemetryd"))
	store := telemetry.NewLogStore(log)
	svc := telemetry.NewPushGateway(log, store)
	// Print data as line protocol
	svc.Encoder = &prometheus.LineProtocol{}

	handler := http.HandlerFunc(svc.Handler)
	log.Info("Starting telemetryd server", zap.String("addr", addr))

	srv := http.Server{
		Addr:     addr,
		Handler:  handler,
		ErrorLog: zap.NewStdLog(log),
	}
	return srv.ListenAndServe()
}
