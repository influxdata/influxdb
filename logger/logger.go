package logger

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/pkg/bytesutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggingBuffer maintains a globally available buffer of the most recent logs.
// It is injected into the server within the influxd main package, and is
// currently accessed by the custom profiling archive generated using the
// /debug/pprof/all URL.
var LoggingBuffer *bytesutil.CircularBuffer

// Stderr should be used whenever any package is logging to os.Stderr. This
// package ensures that any writes to this writer are duplicated to the logging
// buffer defined above.
var Stderr io.Writer

// Stdout is similar to Stderr.
var Stdout io.Writer

func init() {
	size := 1 << 21
	env := os.Getenv("INFLUXDB_PRIVATE_LOG_BUFFER_SIZE")
	if env != "" {
		if n, err := strconv.ParseInt(env, 10, 64); err != nil {
			fmt.Printf("Unable to parse %q as logging buffer size.\n", env)
		} else {
			size = int(n)
		}
	}

	// Initialize LoggingBuffer with 2MB size.
	LoggingBuffer = bytesutil.NewCircularBuffer(size)
	Stderr = io.MultiWriter(LoggingBuffer, os.Stderr)
	Stdout = io.MultiWriter(LoggingBuffer, os.Stdout)
}

// New initializes a new Logger.
func New(w io.Writer) *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.UTC().Format(time.RFC3339))
	}
	config.EncodeDuration = func(d time.Duration, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(d.String())
	}
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(config),
		zapcore.Lock(zapcore.AddSync(w)),
		zapcore.DebugLevel,
	))
}
