package kapacitor

import "testing"
import "github.com/influxdata/chronograf"

func TestInfluxOut(t *testing.T) {
	tests := []struct {
		name string
		want chronograf.TICKScript
	}{
		{
			name: "Test influxDBOut kapacitor node",
			want: `trigger
    |eval(lambda: "emitted")
        .as('value')
        .keep('value', messageField, durationField)
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)
`,
		},
	}
	for _, tt := range tests {
		got, err := InfluxOut(chronograf.AlertRule{
			Name:    "name",
			Trigger: "deadman",
			Query: &chronograf.QueryConfig{
				Fields: []chronograf.Field{
					{
						Name: "mean",
						Type: "func",
						Args: []chronograf.Field{
							{
								Name: "usage_user",
								Type: "field",
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Errorf("%q. InfluxOut()) error = %v", tt.name, err)
			continue
		}
		formatted, err := formatTick(got)
		if err != nil {
			t.Errorf("%q. formatTick() error = %v", tt.name, err)
			continue
		}
		if formatted != tt.want {
			t.Errorf("%q. InfluxOut() = %v, want %v", tt.name, formatted, tt.want)
		}
	}
}
