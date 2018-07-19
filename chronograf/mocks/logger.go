package mocks

import (
	"fmt"
	"io"
	"testing"

	"github.com/influxdata/chronograf"
)

// NewLogger returns a mock logger that implements chronograf.Logger
func NewLogger() chronograf.Logger {
	return &TestLogger{}
}

type LogMessage struct {
	Level string
	Body  string
}

// TestLogger is a chronograf.Logger which allows assertions to be made on the
// contents of its messages.
type TestLogger struct {
	Messages []LogMessage
}

func (tl *TestLogger) Debug(args ...interface{}) {
	tl.Messages = append(tl.Messages, LogMessage{"debug", tl.stringify(args...)})
}

func (tl *TestLogger) Info(args ...interface{}) {
	tl.Messages = append(tl.Messages, LogMessage{"info", tl.stringify(args...)})
}

func (tl *TestLogger) Error(args ...interface{}) {
	tl.Messages = append(tl.Messages, LogMessage{"error", tl.stringify(args...)})
}

func (tl *TestLogger) WithField(key string, value interface{}) chronograf.Logger {
	return tl
}

func (tl *TestLogger) Writer() *io.PipeWriter {
	_, write := io.Pipe()
	return write
}

// HasMessage will return true if the TestLogger has been called with an exact
// match of a particular log message at a particular log level
func (tl *TestLogger) HasMessage(level string, body string) bool {
	for _, msg := range tl.Messages {
		if msg.Level == level && msg.Body == body {
			return true
		}
	}
	return false
}

func (tl *TestLogger) stringify(args ...interface{}) string {
	out := []byte{}
	for _, arg := range args[:len(args)-1] {
		out = append(out, tl.stringifyArg(arg)...)
		out = append(out, []byte(" ")...)
	}
	out = append(out, tl.stringifyArg(args[len(args)-1])...)
	return string(out)
}

func (tl *TestLogger) stringifyArg(arg interface{}) []byte {
	switch a := arg.(type) {
	case fmt.Stringer:
		return []byte(a.String())
	case error:
		return []byte(a.Error())
	case string:
		return []byte(a)
	default:
		return []byte("UNKNOWN")
	}
}

// Dump dumps out logs into a given testing.T's logs
func (tl *TestLogger) Dump(t *testing.T) {
	t.Log("== Dumping Test Logs ==")
	for _, msg := range tl.Messages {
		t.Logf("lvl: %s, msg: %s", msg.Level, msg.Body)
	}
}
