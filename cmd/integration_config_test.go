package cmd_test

import (
	"strconv"
	"strings"
	"testing"

	client "github.com/influxdata/influxdb/client/v2"
)

func TestRetentionAutocreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewTestRunCommand(map[string]string{
				"INFLUXDB_META_RETENTION_AUTOCREATE": strconv.FormatBool(tc.enabled),
			})
			defer cmd.Cleanup()

			cmd.MustRun()

			c, err := client.NewHTTPClient(client.HTTPConfig{
				Addr: "http://" + cmd.BoundHTTPAddr(),
			})
			if err != nil {
				t.Fatal(err)
			}

			_ = mustQuery(c, "CREATE DATABASE test", "", "")

			resp := mustQuery(c, "SHOW RETENTION POLICIES ON test", "", "")
			if len(resp.Results) != 1 {
				t.Fatalf("expected 1 result in response, got %d", len(resp.Results))
			}

			if tc.enabled {
				if len(resp.Results[0].Series) != 1 || len(resp.Results[0].Series[0].Values) != 1 {
					t.Fatalf("expected one automatically created retention policy, got %#v", resp.Results[0].Series[0].Values)
				}
			} else {
				if len(resp.Results[0].Series) != 1 || len(resp.Results[0].Series[0].Values) != 0 {
					t.Fatalf("expected no retention policies, got: %#v", resp.Results[0].Series[0].Values)
				}
			}
		})
	}
}

func TestCacheMaxMemorySize(t *testing.T) {
	cmd := NewTestRunCommand(map[string]string{
		"INFLUXDB_DATA_CACHE_MAX_MEMORY_SIZE": "1024",
	})
	defer cmd.Cleanup()

	cmd.MustRun()

	c := cmd.HTTPClient()
	_ = mustQuery(c, "CREATE DATABASE test", "", "")

	// Add a small point that fits in the cache size.
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{Database: "test"})
	pt, _ := client.NewPoint("strings", nil, map[string]interface{}{"s": "a short string"})
	bp.AddPoint(pt)
	if err := c.Write(bp); err != nil {
		t.Fatal(err)
	}

	// This point won't fit in the cache size and should be rejected.
	bp, _ = client.NewBatchPoints(client.BatchPointsConfig{Database: "test"})
	pt, _ = client.NewPoint("strings", nil, map[string]interface{}{"s": strings.Repeat("long", 1024)})
	bp.AddPoint(pt)
	err := c.Write(bp)
	if err == nil {
		t.Fatal("expected an error but got nil")
	}

	if !strings.Contains(err.Error(), "cache-max-memory-size") {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

func mustQuery(c client.Client, q, db, precision string) *client.Response {
	resp, err := c.Query(client.NewQuery(q, db, precision))
	if err != nil {
		panic(err)
	} else if resp.Error() != nil {
		panic(resp.Error())
	}

	return resp
}
