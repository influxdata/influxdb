package logger

import (
	"go.uber.org/zap/zapcore"
)

type Config struct {
	Format string        `toml:"format"`
	Level  zapcore.Level `toml:"level"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Format: "logfmt",
	}
}
