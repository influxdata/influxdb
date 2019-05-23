package log_control_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/kit/log_control"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogControl(t *testing.T) {
	t.Parallel()

	atom := zap.NewAtomicLevel()
	atom.SetLevel(zapcore.ErrorLevel)

	ts := httptest.NewServer(log_control.NewControl(atom, nil))

	_, err := http.Get(ts.URL + "/debug/log?level=debug&duration=1s")
	if err != nil {
		t.Fatal(err)
	}

	if atom.Level() != zapcore.DebugLevel {
		t.Fatalf("expected %s, got %s", zapcore.DebugLevel.String(), atom.Level().String())
	}

	<-time.After(2 * time.Second)

	if atom.Level() != zapcore.ErrorLevel {
		t.Fatalf("expected %s, got %s", zapcore.ErrorLevel.String(), atom.Level().String())
	}

}
