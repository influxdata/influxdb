package logger

import (
	"fmt"
	"io"
	"time"

	zaplogfmt "github.com/jsternberg/zap-logfmt"
	isatty "github.com/mattn/go-isatty"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const TimeFormat = "2006-01-02T15:04:05.000000Z07:00"

func New(w io.Writer) *zap.Logger {
	config := NewConfig()
	l, _ := config.New(w)
	return l
}

func (c *Config) New(defaultOutput io.Writer) (*zap.Logger, error) {
	w := defaultOutput
	format := c.Format
	if format == "console" {
		// Disallow the console logger if the output is not a terminal.
		return nil, fmt.Errorf("unknown logging format: %s", format)
	}

	// If the format is empty or auto, then set the format depending
	// on whether or not a terminal is present.
	if format == "" || format == "auto" {
		if IsTerminal(w) {
			format = "console"
		} else {
			format = "logfmt"
		}
	}

	encoder, err := newEncoder(format)
	if err != nil {
		return nil, err
	}
	return zap.New(zapcore.NewCore(
		encoder,
		zapcore.Lock(zapcore.AddSync(w)),
		c.Level,
	), zap.Fields(zap.String("log_id", nextID()))), nil
}

func newEncoder(format string) (zapcore.Encoder, error) {
	config := newEncoderConfig()
	switch format {
	case "json":
		return zapcore.NewJSONEncoder(config), nil
	case "console":
		return zapcore.NewConsoleEncoder(config), nil
	case "logfmt":
		return zaplogfmt.NewEncoder(config), nil
	default:
		return nil, fmt.Errorf("unknown logging format: %s", format)
	}
}

func newEncoderConfig() zapcore.EncoderConfig {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.UTC().Format(TimeFormat))
	}
	config.EncodeDuration = func(d time.Duration, encoder zapcore.PrimitiveArrayEncoder) {
		val := float64(d) / float64(time.Millisecond)
		encoder.AppendString(fmt.Sprintf("%.3fms", val))
	}
	config.LevelKey = "lvl"
	return config
}

// IsTerminal checks if w is a file and whether it is an interactive terminal session.
func IsTerminal(w io.Writer) bool {
	if f, ok := w.(interface {
		Fd() uintptr
	}); ok {
		return isatty.IsTerminal(f.Fd())
	}
	return false
}

const (
	year = 365 * 24 * time.Hour
	week = 7 * 24 * time.Hour
	day  = 24 * time.Hour
)

func DurationLiteral(key string, val time.Duration) zapcore.Field {
	if val == 0 {
		return zap.String(key, "0s")
	}

	var (
		value int
		unit  string
	)
	switch {
	case val%year == 0:
		value = int(val / year)
		unit = "y"
	case val%week == 0:
		value = int(val / week)
		unit = "w"
	case val%day == 0:
		value = int(val / day)
		unit = "d"
	case val%time.Hour == 0:
		value = int(val / time.Hour)
		unit = "h"
	case val%time.Minute == 0:
		value = int(val / time.Minute)
		unit = "m"
	case val%time.Second == 0:
		value = int(val / time.Second)
		unit = "s"
	default:
		value = int(val / time.Millisecond)
		unit = "ms"
	}
	return zap.String(key, fmt.Sprintf("%d%s", value, unit))
}
