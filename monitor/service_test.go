package monitor

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// Test that a registered stats client results in the correct SHOW STATS output.
func Test_RegisterStats(t *testing.T) {
	monitor := openMonitor(t)
	executor := &StatementExecutor{Monitor: monitor}

	// Register stats without tags.
	statMap := influxdb.NewStatistics("foo", "foo", nil)
	statMap.Add("bar", 1)
	statMap.AddFloat("qux", 2.4)
	json := executeShowStatsJSON(t, executor)
	if !strings.Contains(json, `"columns":["bar","qux"],"values":[[1,2.4]]`) || !strings.Contains(json, `"name":"foo"`) {
		t.Fatalf("SHOW STATS response incorrect, got: %s\n", json)
	}

	// Register a client with tags.
	statMap = influxdb.NewStatistics("bar", "baz", map[string]string{"proto": "tcp"})
	statMap.Add("bar", 1)
	statMap.AddFloat("qux", 2.4)
	json = executeShowStatsJSON(t, executor)
	if !strings.Contains(json, `"columns":["bar","qux"],"values":[[1,2.4]]`) ||
		!strings.Contains(json, `"name":"baz"`) ||
		!strings.Contains(json, `"proto":"tcp"`) {
		t.Fatalf("SHOW STATS response incorrect, got: %s\n", json)

	}
}

type mockMetaClient struct{}

func (m *mockMetaClient) ClusterID() uint64                                     { return 1 }
func (m *mockMetaClient) IsLeader() bool                                        { return true }
func (m *mockMetaClient) SetDefaultRetentionPolicy(database, name string) error { return nil }
func (m *mockMetaClient) DropRetentionPolicy(database, name string) error       { return nil }
func (m *mockMetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *mockMetaClient) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return nil, nil
}

func openMonitor(t *testing.T) *Monitor {
	monitor := New(NewConfig())
	monitor.MetaClient = &mockMetaClient{}
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
