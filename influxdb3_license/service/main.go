package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/config"
	"github.com/mattn/go-isatty"
	"go.uber.org/zap"
)

func main() {
	// Get configuration from command line and environment variables
	config, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("Error reading configuration: %v", err)
	}

	// Create logger
	logger, err := createLogger(config.LogFormat, config.LogLevel)
	if err != nil {
		log.Fatalf("Error creating logger: %v", err)
	}

	logger.Info("InfluxDB Pro license service started")
	defer logger.Info("InfluxDB Pro license service stopped")

	// Create handler
	var (
		store  Storer
		mail   Mailer
		licmkr LicenseMaker
		licsgr LicenseSigner
	)

	handler := NewHTTPHandler(config, logger, store, mail, licmkr, licsgr)

	// Start HTTP server
	logger.Info("Starting HTTP server", zap.String("addr", config.HTTPAddr))
	if err := http.ListenAndServe(config.HTTPAddr, handler); err != nil {
		logger.Error("Error starting HTTP server", zap.Error(err))
	}
}

// createLogger creates a new logger with the given configuration.
func createLogger(format, level string) (*zap.Logger, error) {
	var cfg zap.Config
	switch format {
	case "logfmt":
		cfg = zap.NewProductionConfig()
		cfg.Encoding = "console"
	case "json":
		cfg = zap.NewProductionConfig()
		cfg.Encoding = "json"
	case "auto":
		if isatty.IsTerminal(os.Stdout.Fd()) {
			cfg = zap.NewDevelopmentConfig()
		} else {
			cfg = zap.NewProductionConfig()
		}
		cfg.Encoding = "console"
	default:
		return nil, fmt.Errorf("unknown log format: %s", format)
	}

	switch level {
	case "error":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	default:
		return nil, fmt.Errorf("unknown log level: %s", level)
	}

	logger, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	return logger, nil
}
