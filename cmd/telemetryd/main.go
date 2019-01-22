package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/influxdata/influxdb/kit/cli"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/telemetry"
	"go.uber.org/zap"
)

var (
	logger = influxlogger.New(os.Stdout)
	addr   string
)

func main() {
	prog := &cli.Program{
		Run:  run,
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
	cmd := cli.NewCommand(prog)

	var exitCode int
	if err := cmd.Execute(); err != nil {
		exitCode = 1
		logger.Error("Command returned error", zap.Error(err))
	}

	if err := logger.Sync(); err != nil {
		exitCode = 1
		fmt.Fprintf(os.Stderr, "Error syncing logs: %v\n", err)
	}
	time.Sleep(10 * time.Millisecond)
	os.Exit(exitCode)
}

func run() error {
	logger := logger.With(zap.String("service", "telemetryd"))
	store := &telemetry.LogStore{
		Logger: logger,
	}
	svc := telemetry.NewPushGateway(logger, store)
	// Print data as line protocol
	svc.Encoder = &prometheus.LineProtocol{}

	handler := http.HandlerFunc(svc.Handler)
	logger.Info("starting telemetryd server", zap.String("addr", addr))

	srv := http.Server{
		Addr:     addr,
		Handler:  handler,
		ErrorLog: zap.NewStdLog(logger),
	}
	return srv.ListenAndServe()
}
