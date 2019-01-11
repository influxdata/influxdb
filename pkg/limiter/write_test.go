package limiter_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
)

func TestWriter_Limited(t *testing.T) {
	r := bytes.NewReader(bytes.Repeat([]byte{0}, 1024*1024))

	limit := 512 * 1024
	w := limiter.NewWriter(discardCloser{}, limit, 10*1024*1024)

	start := time.Now()
	n, err := io.Copy(w, r)
	elapsed := time.Since(start)
	if err != nil {
		t.Error("copy error: ", err)
	}

	rate := float64(n) / elapsed.Seconds()
	if rate > float64(limit) {
		t.Errorf("rate limit mismath: exp %f, got %f", float64(limit), rate)
	}
}

type discardCloser struct{}

func (d discardCloser) Write(b []byte) (int, error) { return len(b), nil }
func (d discardCloser) Close() error                { return nil }
