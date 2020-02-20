package flub_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal/flub"
)

func TestSimpleScript(t *testing.T) {
	f := flub.NewFile("main")
	f.From("telegraf").Range(-time.Minute).Yield("last_minute")
	f.From("telegraf").Range(-time.Hour).Yield("last_hour")
	fmt.Println(f.Format())
}
