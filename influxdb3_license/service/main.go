package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/email"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license/signer"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store/postgres"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/config"
	"github.com/mattn/go-isatty"
	"go.uber.org/zap"
)

func main() {
	// Get configuration from command line and environment variables
	config, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("error reading configuration: %v", err)
	}

	// Create logger
	logger, err := createLogger(config.LogFormat, config.LogLevel)
	if err != nil {
		log.Fatalf("error creating logger: %v", err)
	}

	logger.Info("InfluxDB Pro license service started")
	defer logger.Info("InfluxDB Pro license service stopped")

	// Open the database store
	stor, err := newStore(config)
	if err != nil {
		logger.Fatal("error opening database", zap.String("db_conn_str", config.DBConnString), zap.Error(err))
	}

	// Create an email service
	emailSvc := newEmailService(config, stor, logger)

	// Create a license creator / signer
	lic, err := newLicenseCreator(config)
	if err != nil {
		logger.Fatal("error creating license creator / signer", zap.Error(err))
	}

	// Create the license service HTTP API handler
	handler := NewHTTPHandler(config, logger, stor, emailSvc, lic)

	// Start HTTP server
	logger.Info("starting HTTP server", zap.String("addr", config.HTTPAddr))
	if err := http.ListenAndServe(config.HTTPAddr, handler); err != nil {
		logger.Error("Error starting HTTP server", zap.Error(err))
	}
}

// newStore opens a connection to the database store and returns it.
func newStore(cfg *config.Config) (store.Store, error) {
	db, err := sql.Open("postgres", cfg.DBConnString)
	if err != nil {
		return nil, err
	}

	return postgres.NewStore(db), nil
}

// newEmailService creates a new mail service
func newEmailService(c *config.Config, s store.Store, l *zap.Logger) *email.Service {
	// Create a client for whatever email service we are using
	var mailer email.Mailer
	if c.EmailAPIKey == "log-only" {
		// Use a debug mailer that logs instead of sending emails
		mailer = email.NewDebugMailer(l)
	} else {
		// Use Mailgun - that's the only one we support right now
		mailer = email.NewMailgun(c.EmailDomain, c.EmailAPIKey)
	}

	emailCfg := email.DefaultConfig(mailer, s, l)
	emailCfg.Domain = c.EmailDomain
	emailCfg.EmailTemplateName = c.EmailTemplateName
	emailCfg.MaxSendAttempts = c.EmailMaxRetries
	emailCfg.MaxEmailsPerUserPerLicense = c.EmailMaxRetries

	svc := email.NewService(emailCfg)
	svc.Start(time.Second)

	return svc
}

// newLicenseCreator creates an instance of a new license creator/signer
func newLicenseCreator(c *config.Config) (*license.Creator, error) {
	s := signer.NewKMSSigningMethod()
	lc, err := license.NewCreator(s, c.PrivateKey, c.PublicKey)
	if err != nil {
		return nil, err
	}
	return lc, nil
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
