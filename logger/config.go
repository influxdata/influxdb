package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config represents the configuration for creating a zap.Logger.
type Config struct {
	Format       string          `toml:"format"`
	Level        zap.AtomicLevel `toml:"level"`
	SuppressLogo bool            `toml:"suppress-logo"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Format: "auto",
		Level:  zap.NewAtomicLevelAt(zapcore.InfoLevel),
	}
}
