package query

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultRemoteNodeTimeout is the default timeout within which a remote node must respond
	// to query request.
	DefaultRemoteNodeTimeout = 5 * time.Second
)

type Config struct {
	RemoteNodeTimeout toml.Duration `toml:"remote-node-timeout"`
}

func NewConfig() Config {
	return Config{
		RemoteNodeTimeout: toml.Duration(DefaultRemoteNodeTimeout),
	}
}
