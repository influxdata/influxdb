package monitor_test

import (
	"bytes"
	"expvar"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMonitor_Open(t *testing.T) {
	s := monitor.New(nil, monitor.Config{})
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}

	// Verify that opening twice is fine.
	if err := s.Open(); err != nil {
		s.Close()
		t.Fatalf("unexpected error on second open: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("unexpected close error: %s", err)
	}

	// Verify that closing twice is fine.
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error on second close: %s", err)
	}
}

func TestMonitor_SetPointsWriter_StoreEnabled(t *testing.T) {
	var mc MetaClient
	mc.CreateDatabaseWithRetentionPolicyFn = func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		return &meta.DatabaseInfo{Name: name}, nil
	}

	config := monitor.NewConfig()
	s := monitor.New(nil, config)
	s.MetaClient = &mc
	core, logs := observer.New(zap.DebugLevel)
	s.WithLogger(zap.New(core))

	// Setting the points writer should open the monitor.
	var pw PointsWriter
	if err := s.SetPointsWriter(&pw); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	// Verify that the monitor was opened by looking at the log messages.
	if logs.FilterMessage("Starting monitor service").Len() == 0 {
		t.Errorf("monitor system was never started")
	}
}

func TestMonitor_SetPointsWriter_StoreDisabled(t *testing.T) {
	s := monitor.New(nil, monitor.Config{})
	core, logs := observer.New(zap.DebugLevel)
	s.WithLogger(zap.New(core))

	// Setting the points writer should open the monitor.
	var pw PointsWriter
	if err := s.SetPointsWriter(&pw); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	// Verify that the monitor was not opened by looking at the log messages.
	if logs.FilterMessage("Starting monitor system").Len() > 0 {
		t.Errorf("monitor system should not have been started")
	}
}

func TestMonitor_StoreStatistics(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	ch := make(chan models.Points)

	var mc MetaClient
	mc.CreateDatabaseWithRetentionPolicyFn = func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		if got, want := name, monitor.DefaultStoreDatabase; got != want {
			t.Errorf("unexpected database: got=%q want=%q", got, want)
		}
		if got, want := spec.Name, monitor.MonitorRetentionPolicy; got != want {
			t.Errorf("unexpected retention policy: got=%q want=%q", got, want)
		}
		if spec.Duration != nil {
			if got, want := *spec.Duration, monitor.MonitorRetentionPolicyDuration; got != want {
				t.Errorf("unexpected duration: got=%q want=%q", got, want)
			}
		} else {
			t.Error("expected duration in retention policy spec")
		}
		if spec.ReplicaN != nil {
			if got, want := *spec.ReplicaN, monitor.MonitorRetentionPolicyReplicaN; got != want {
				t.Errorf("unexpected replica number: got=%q want=%q", got, want)
			}
		} else {
			t.Error("expected replica number in retention policy spec")
		}
		return &meta.DatabaseInfo{Name: name}, nil
	}

	var pw PointsWriter
	pw.WritePointsFn = func(database, policy string, points models.Points) error {
		// Verify that we are attempting to write to the correct database.
		if got, want := database, monitor.DefaultStoreDatabase; got != want {
			t.Errorf("unexpected database: got=%q want=%q", got, want)
		}
		if got, want := policy, monitor.MonitorRetentionPolicy; got != want {
			t.Errorf("unexpected retention policy: got=%q want=%q", got, want)
		}

		// Attempt to write the points to the main goroutine.
		select {
		case <-done:
		case ch <- points:
		}
		return nil
	}

	config := monitor.NewConfig()
	config.StoreInterval = toml.Duration(10 * time.Millisecond)
	s := monitor.New(nil, config)
	s.MetaClient = &mc
	s.PointsWriter = &pw

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case points := <-ch:
		timer.Stop()

		// Search for the runtime statistic.
		found := false
		for _, pt := range points {
			if !bytes.Equal(pt.Name(), []byte("runtime")) {
				continue
			}

			// There should be a hostname.
			if got := pt.Tags().GetString("hostname"); len(got) == 0 {
				t.Errorf("expected hostname tag")
			}
			// This should write on an exact interval of 10 milliseconds.
			if got, want := pt.Time(), pt.Time().Truncate(10*time.Millisecond); got != want {
				t.Errorf("unexpected time: got=%q want=%q", got, want)
			}
			found = true
			break
		}

		if !found {
			t.Error("unable to find runtime statistic")
		}
	case <-timer.C:
		t.Errorf("timeout while waiting for statistics to be written")
	}
}

func TestMonitor_Reporter(t *testing.T) {
	reporter := ReporterFunc(func(tags map[string]string) []models.Statistic {
		return []models.Statistic{
			{
				Name: "foo",
				Tags: tags,
				Values: map[string]interface{}{
					"value": "bar",
				},
			},
		}
	})

	done := make(chan struct{})
	defer close(done)
	ch := make(chan models.Points)

	var mc MetaClient
	mc.CreateDatabaseWithRetentionPolicyFn = func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		return &meta.DatabaseInfo{Name: name}, nil
	}

	var pw PointsWriter
	pw.WritePointsFn = func(database, policy string, points models.Points) error {
		// Attempt to write the points to the main goroutine.
		select {
		case <-done:
		case ch <- points:
		}
		return nil
	}

	config := monitor.NewConfig()
	config.StoreInterval = toml.Duration(10 * time.Millisecond)
	s := monitor.New(reporter, config)
	s.MetaClient = &mc
	s.PointsWriter = &pw

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case points := <-ch:
		timer.Stop()

		// Look for the statistic.
		found := false
		for _, pt := range points {
			if !bytes.Equal(pt.Name(), []byte("foo")) {
				continue
			}
			found = true
			break
		}

		if !found {
			t.Error("unable to find foo statistic")
		}
	case <-timer.C:
		t.Errorf("timeout while waiting for statistics to be written")
	}
}

func expvarMap(name string, tags map[string]string, fields map[string]interface{}) *expvar.Map {
	m := new(expvar.Map).Init()
	eName := new(expvar.String)
	eName.Set(name)
	m.Set("name", eName)

	var eTags *expvar.Map
	if len(tags) > 0 {
		eTags = new(expvar.Map).Init()
		for k, v := range tags {
			kv := new(expvar.String)
			kv.Set(v)
			eTags.Set(k, kv)
		}
		m.Set("tags", eTags)
	}

	var eFields *expvar.Map
	if len(fields) > 0 {
		eFields = new(expvar.Map).Init()
		for k, v := range fields {
			switch v := v.(type) {
			case float64:
				kv := new(expvar.Float)
				kv.Set(v)
				eFields.Set(k, kv)
			case int:
				kv := new(expvar.Int)
				kv.Set(int64(v))
				eFields.Set(k, kv)
			case string:
				kv := new(expvar.String)
				kv.Set(v)
				eFields.Set(k, kv)
			}
		}
		m.Set("values", eFields)
	}
	return m
}

func TestMonitor_Expvar(t *testing.T) {
	done := make(chan struct{})
	var once sync.Once
	// Ensure the done channel will always be closed by calling this early.
	defer once.Do(func() { close(done) })
	ch := make(chan models.Points)

	var mc MetaClient
	mc.CreateDatabaseWithRetentionPolicyFn = func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		return &meta.DatabaseInfo{Name: name}, nil
	}

	var pw PointsWriter
	pw.WritePointsFn = func(database, policy string, points models.Points) error {
		// Attempt to write the points to the main goroutine.
		select {
		case <-done:
		case ch <- points:
		}
		return nil
	}

	config := monitor.NewConfig()
	config.StoreInterval = toml.Duration(10 * time.Millisecond)
	s := monitor.New(nil, config)
	s.MetaClient = &mc
	s.PointsWriter = &pw

	expvar.Publish("expvar1", expvarMap(
		"expvar1",
		map[string]string{
			"region": "uswest2",
		},
		map[string]interface{}{
			"value": 2.0,
		},
	))
	expvar.Publish("expvar2", expvarMap(
		"expvar2",
		map[string]string{
			"region": "uswest2",
		},
		nil,
	))
	expvar.Publish("expvar3", expvarMap(
		"expvar3",
		nil,
		map[string]interface{}{
			"value": 2,
		},
	))

	bad := new(expvar.String)
	bad.Set("badentry")
	expvar.Publish("expvar4", bad)

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()
	// Call this again here. Since defers run in first in, last out order, we want to close
	// the done channel before we call close on the monitor. This prevents a deadlock in the test.
	defer once.Do(func() { close(done) })

	hostname, _ := os.Hostname()
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case points := <-ch:
		timer.Stop()

		// Look for the statistic.
		var found1, found3 bool
		for _, pt := range points {
			if bytes.Equal(pt.Name(), []byte("expvar1")) {
				if got, want := pt.Tags().HashKey(), []byte(fmt.Sprintf(",hostname=%s,region=uswest2", hostname)); !reflect.DeepEqual(got, want) {
					t.Errorf("unexpected expvar1 tags: got=%v want=%v", string(got), string(want))
				}
				fields, _ := pt.Fields()
				if got, want := fields, models.Fields(map[string]interface{}{
					"value": 2.0,
				}); !reflect.DeepEqual(got, want) {
					t.Errorf("unexpected expvar1 fields: got=%v want=%v", got, want)
				}
				found1 = true
			} else if bytes.Equal(pt.Name(), []byte("expvar2")) {
				t.Error("found expvar2 statistic")
			} else if bytes.Equal(pt.Name(), []byte("expvar3")) {
				if got, want := pt.Tags().HashKey(), []byte(fmt.Sprintf(",hostname=%s", hostname)); !reflect.DeepEqual(got, want) {
					t.Errorf("unexpected expvar3 tags: got=%v want=%v", string(got), string(want))
				}
				fields, _ := pt.Fields()
				if got, want := fields, models.Fields(map[string]interface{}{
					"value": int64(2),
				}); !reflect.DeepEqual(got, want) {
					t.Errorf("unexpected expvar3 fields: got=%v want=%v", got, want)
				}
				found3 = true
			}
		}

		if !found1 {
			t.Error("unable to find expvar1 statistic")
		}
		if !found3 {
			t.Error("unable to find expvar3 statistic")
		}
	case <-timer.C:
		t.Errorf("timeout while waiting for statistics to be written")
	}
}

func TestMonitor_QuickClose(t *testing.T) {
	var mc MetaClient
	mc.CreateDatabaseWithRetentionPolicyFn = func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		return &meta.DatabaseInfo{Name: name}, nil
	}

	var pw PointsWriter
	config := monitor.NewConfig()
	config.StoreInterval = toml.Duration(24 * time.Hour)
	s := monitor.New(nil, config)
	s.MetaClient = &mc
	s.PointsWriter = &pw

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestStatistic_ValueNames(t *testing.T) {
	statistic := monitor.Statistic{
		Statistic: models.Statistic{
			Name: "foo",
			Values: map[string]interface{}{
				"abc": 1.0,
				"def": 2.0,
			},
		},
	}

	names := statistic.ValueNames()
	if got, want := names, []string{"abc", "def"}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected value names: got=%v want=%v", got, want)
	}
}

func TestStatistics_Sort(t *testing.T) {
	statistics := []*monitor.Statistic{
		{Statistic: models.Statistic{Name: "b"}},
		{Statistic: models.Statistic{Name: "a"}},
		{Statistic: models.Statistic{Name: "c"}},
	}

	sort.Sort(monitor.Statistics(statistics))
	names := make([]string, 0, len(statistics))
	for _, stat := range statistics {
		names = append(names, stat.Name)
	}

	if got, want := names, []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect sorting of statistics: got=%v want=%v", got, want)
	}
}

type ReporterFunc func(tags map[string]string) []models.Statistic

func (f ReporterFunc) Statistics(tags map[string]string) []models.Statistic {
	return f(tags)
}

type PointsWriter struct {
	WritePointsFn func(database, policy string, points models.Points) error
}

func (pw *PointsWriter) WritePoints(database, policy string, points models.Points) error {
	if pw.WritePointsFn != nil {
		return pw.WritePointsFn(database, policy, points)
	}
	return nil
}

type MetaClient struct {
	CreateDatabaseWithRetentionPolicyFn func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	DatabaseFn                          func(name string) *meta.DatabaseInfo
}

func (m *MetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	return m.CreateDatabaseWithRetentionPolicyFn(name, spec)
}

func (m *MetaClient) Database(name string) *meta.DatabaseInfo {
	if m.DatabaseFn != nil {
		return m.DatabaseFn(name)
	}
	return nil
}
