package upgrade

import (
	"net/url"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

func init() {
	// NOTE: Work around a bug in zap's handling of absolute paths on Windows.
	// See https://github.com/uber-go/zap/issues/621
	newWinFileSink := func(u *url.URL) (zap.Sink, error) {
		// Remove leading slash left by url.Parse()
		return os.OpenFile(u.Path[1:], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	}
	zap.RegisterSink("winfile", newWinFileSink)
}

func buildLogger(options *logOptions, verbose bool) (*zap.Logger, error) {
	config := zap.NewProductionConfig()

	config.Level = zap.NewAtomicLevelAt(options.logLevel)
	if verbose {
		config.Level.SetLevel(zap.DebugLevel)
	}

	logPath := "winfile:///" + filepath.Abs(options.logPath)
	config.OutputPaths = append(config.OutputPaths, logPath)
	config.ErrorOutputPaths = append(config.ErrorOutputPaths, logPath)

	log, err := config.Build()
	if err != nil {
		return nil, err
	}
	if verbose {
		log.Warn("--verbose is deprecated, use --log-level=debug instead")
	}
	return log, nil
}
