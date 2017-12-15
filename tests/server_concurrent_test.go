package tests

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestConcurrentServer_WriteValues(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Not implemented on remote server")
	}

	// The first %%d becomes a %d once fmt is done, so we can then inject new
	// measurement names later on.
	write := strings.Join([]string{
		fmt.Sprintf(`a%%[1]d val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`b%%[1]d val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`c%%[1]d val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`d%%[1]d val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}, "\n")

	var i int64
	var f1 = func() {
		s.Write("db0", "rp0", fmt.Sprintf(write, i), nil)
	}

	var f2 = func() { s.DropDatabase("db0") }
	runTest(10*time.Second, f1, f2)
}

func TestConcurrentServer_TagValues(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Not implemented on remote server")
	}

	write := strings.Join([]string{
		fmt.Sprintf(`a,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=useast val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=ussouth val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=usnorth val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}, "\n")

	var f1 = func() {
		s.Write("db0", "rp0", write, nil)
	}

	stmt, err := influxql.ParseStatement(`SHOW TAG VALUES WITH KEY = "region"`)
	if err != nil {
		t.Fatal(err)
	}
	rewrite, err := query.RewriteStatement(stmt)
	if err != nil {
		t.Fatal(err)
	}

	cond := rewrite.(*influxql.ShowTagValuesStatement).Condition
	var f2 = func() {
		srv, ok := s.(*LocalServer)
		if !ok {
			t.Fatal("Not a local server")
		}

		sgis, err := s.(*LocalServer).MetaClient.ShardGroupsByTimeRange("db0", "rp0", time.Time{}, time.Time{})
		if err != nil {
			return
		}

		var ids []uint64
		for _, sgi := range sgis {
			for _, si := range sgi.Shards {
				ids = append(ids, si.ID)
			}
		}
		srv.TSDBStore.TagValues(nil, ids, cond)
	}

	var f3 = func() { s.DropDatabase("db0") }
	runTest(10*time.Second, f1, f2, f3)
}

func TestConcurrentServer_ShowMeasurements(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Not implemented on remote server")
	}

	write := strings.Join([]string{
		fmt.Sprintf(`a,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=useast val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=ussouth val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`a,host=serverA,region=usnorth val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}, "\n")

	var f1 = func() {
		s.Write("db0", "rp0", write, nil)
		s.DropDatabase("db0")
	}

	var f2 = func() {
		srv, ok := s.(*LocalServer)
		if !ok {
			t.Fatal("Not a local server")
		}
		srv.TSDBStore.MeasurementNames(query.OpenAuthorizer, "db0", nil)
	}

	runTest(10*time.Second, f1, f2)
}

// runTest continuously, and concurrently, runs the provided functions. No error
// checking is performed, runTest is simply trying to find panics.
func runTest(d time.Duration, fns ...func()) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(d)
		<-timer.C
		close(done)
		timer.Stop()
	}()

	for _, fn := range fns {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					f()
				}
			}
		}(fn)
	}

	wg.Wait()
}
