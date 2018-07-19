package queries

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	tests := []struct {
		q string
	}{
		{q: fmt.Sprintf(`SELECT mean(field1), sum(field2) ,count(field3::field) AS field_x FROM myseries WHERE host = 'hosta.influxdb.org' and time > '%s' GROUP BY time(10h) ORDER BY DESC LIMIT 20 OFFSET 10;`, time.Now().UTC().Format(time.RFC3339Nano))},
		{q: fmt.Sprintf(`SELECT difference(max(field1)) FROM myseries WHERE time > '%s' GROUP BY time(1m)`, time.Now().UTC().Format(time.RFC3339Nano))},
		{q: `SELECT derivative(field1, 1h) / derivative(field2, 1h) FROM myseries`},
		{q: `SELECT mean("load1") FROM "system" WHERE "cluster_id" =~ /^$ClusterID$/ AND time > now() - 1h GROUP BY time(10m), "host" fill(null)`},
		{q: "SELECT max(\"n_cpus\") AS \"max_cpus\", non_negative_derivative(median(\"n_users\"), 5m) FROM \"system\" WHERE \"cluster_id\" =~ /^23/ AND \"host\" = 'prod-2ccccc04-us-east-1-data-3' AND time > now() - 15m GROUP BY time(15m, 10s),host,tag_x fill(10)"},
		{q: "SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"default\".\"cpu\" WHERE host =~ /\\./ AND time > now() - 1h"},
		{q: `SELECT 1 + "A" FROM howdy`},
	}

	for i, tt := range tests {
		stmt, err := ParseSelect(tt.q)
		if err != nil {
			t.Errorf("Test %d query %s invalid statement: %v", i, tt.q, err)
		}
		_, err = json.MarshalIndent(stmt, "", "    ")
		if err != nil {
			t.Errorf("Test %d query %s Unable to marshal statement: %v", i, tt.q, err)
		}
	}
}
