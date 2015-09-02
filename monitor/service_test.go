package monitor

import (
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

// Test that a registered stats client results in the correct SHOW STATS output.
func Test_RegisterStats(t *testing.T) {
	monitor := openMonitor(t)
	executor := &StatementExecutor{Monitor: monitor}

	client := mockStatsClient{
		StatisticsFn: func() (map[string]interface{}, error) {
			return map[string]interface{}{
				"bar": 1,
				"qux": 2.4,
			}, nil
		},
	}

	// Register a client without tags.
	if err := monitor.Register("foo", nil, client); err != nil {
		t.Fatalf("failed to register client: %s", err.Error())
	}
	json := executeShowStatsJSON(t, executor)
	if !strings.Contains(json, `{"name":"foo","columns":["bar","qux"],"values":[[1,2.4]]}]}`) {
		t.Fatalf("SHOW STATS response incorrect, got: %s\n", json)
	}

	// Register a client with tags.
	if err := monitor.Register("baz", map[string]string{"proto": "tcp"}, client); err != nil {
		t.Fatalf("failed to register client: %s", err.Error())
	}
	json = executeShowStatsJSON(t, executor)
	if !strings.Contains(json, `{"name":"baz","tags":{"proto":"tcp"},"columns":["bar","qux"],"values":[[1,2.4]]}]}`) {
		t.Fatalf("SHOW STATS response incorrect, got: %s\n", json)
	}
}

type mockStatsClient struct {
	StatisticsFn func() (map[string]interface{}, error)
}

func (m mockStatsClient) Statistics() (map[string]interface{}, error) {
	return m.StatisticsFn()
}

func (m mockStatsClient) Diagnostics() (map[string]interface{}, error) {
	return nil, nil
}

type mockMetastore struct{}

func (m *mockMetastore) ClusterID() (uint64, error)          { return 1, nil }
func (m *mockMetastore) NodeID() uint64                      { return 2 }
func (m *mockMetastore) WaitForLeader(d time.Duration) error { return nil }
func (m *mockMetastore) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}

func openMonitor(t *testing.T) *Monitor {
	monitor := New(NewConfig())
	monitor.MetaStore = &mockMetastore{}
	err := monitor.Open()
	if err != nil {
		t.Fatalf("failed to open monitor: %s", err.Error())
	}
	return monitor
}

func executeShowStatsJSON(t *testing.T, s *StatementExecutor) string {
	r := s.ExecuteStatement(&influxql.ShowStatsStatement{})
	b, err := r.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to decode SHOW STATS response: %s", err.Error())
	}
	return string(b)
}
