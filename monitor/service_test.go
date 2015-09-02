package monitor

import (
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Test that a registered stats client results in the correct SHOW STATS output.
func Test_ServiceRegisterStats(t *testing.T) {
	service := openService(t)

	client := mockStatsClient{
		StatisticsFn: func() (map[string]interface{}, error) {
			return map[string]interface{}{
				"bar": 1,
				"qux": 2.4,
			}, nil
		},
	}

	// Register a client without tags.
	if err := service.Register("foo", nil, client); err != nil {
		t.Fatalf("failed to register client: %s", err.Error())
	}
	json := executeShowStatsJSON(t, service)
	if !strings.Contains(json, `{"name":"foo","columns":["bar","qux"],"values":[[1,2.4]]}]}`) {
		t.Fatalf("SHOW STATS response incorrect, got: %s\n", json)
	}

	// Register a client with tags.
	if err := service.Register("baz", map[string]string{"proto": "tcp"}, client); err != nil {
		t.Fatalf("failed to register client: %s", err.Error())
	}
	json = executeShowStatsJSON(t, service)
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

func openService(t *testing.T) *Service {
	service := NewService(NewConfig())
	err := service.Open(1, 2, "serverA")
	if err != nil {
		t.Fatalf("failed to open service: %s", err.Error())
	}
	return service
}

func executeShowStatsJSON(t *testing.T, s *Service) string {
	r := s.ExecuteStatement(&influxql.ShowStatsStatement{})
	b, err := r.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to decode SHOW STATS response: %s", err.Error())
	}
	return string(b)
}
