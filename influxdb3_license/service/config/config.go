package config

import (
	"github.com/alecthomas/kong"
)

// Config represents the configuration of the service
type Config struct {
	HTTPAddr  string `help:"Address:port of the HTTP API" env:"IFLX_PRO_LIC_HTTP_ADDR" default:":8080"`
	LogLevel  string `help:"Log level: error, warn, info (default), debug" env:"IFLX_PRO_LIC_LOG_LEVEL" default:"info"`
	LogFormat string `help:"Log format: auto, logfmt, json" env:"IFLX_PRO_LIC_LOG_FORMAT" default:"auto"`
}

// Parse parses the config from environment vars and command line arguments.
// The order of precedence is:
//  1. Command line arguments
//  2. Environment variables
func Parse(args []string) (*Config, error) {
	config := &Config{}

	// Create a new Kong parser
	parser, err := kong.New(config)
	if err != nil {
		return nil, err
	}

	// Parse the command line arguments
	_, err = parser.Parse(args)
	if err != nil {
		return nil, err
	}

	return config, nil
}
