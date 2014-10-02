package log_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdb/influxdb/log"
)

// Ensure the log can write messages to the appropriate topics.
func TestLog_Write(t *testing.T) {
	l := NewLog()
	defer l.Close()

	// Write a message to the log.
	l.Publish("foo/bar")
}

// Log is a wrapper for log.Log that creates the log in a temporary location.
type Log struct {
	*log.Log
}

// NewLog returns a new open tempoarary log.
func NewLog() *Log {
	l := log.NewLog()
	if err := l.Open(tempfile()); err != nil {
		panic("open: " + err.Error())
	}
	return &Log{l}
}

// Close closes and deletes the temporary log.
func (l *Log) Close() {
	defer os.RemoveAll(l.Path())
	l.Log.Close()
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-log-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
