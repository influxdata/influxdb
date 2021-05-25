package upgrade

import (
	"net/url"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// Work around a bug in zap's handling of absolute paths on Windows.
// See https://github.com/uber-go/zap/issues/621

const FakeWindowsScheme = "winfile"

func init() {
	newWinFileSink := func(u *url.URL) (zap.Sink, error) {
		// Remove leading slash left by url.Parse()
		return os.OpenFile(u.Path[1:], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	}
	zap.RegisterSink(FakeWindowsScheme, newWinFileSink)
}

func (o *logOptions) zapSafeLogPath() (string, error) {
	logPath, err := filepath.Abs(o.logPath)
	if err != nil {
		return "", err
	}
	return FakeWindowsScheme + ":///" + logPath, nil
}
