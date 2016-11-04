package kapacitor

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestAlertServices(t *testing.T) {
	tests := []struct {
		name    string
		rule    chronograf.AlertRule
		want    chronograf.TICKScript
		wantErr bool
	}{
		{
			name: "Test several valid services",
			rule: chronograf.AlertRule{
				Alerts: []string{"slack", "victorops", "email"},
			},
			want: `alert()
        .slack()
        .victorOps()
        .email()
`,
		},
		{
			name: "Test single invalid services amongst several valid",
			rule: chronograf.AlertRule{
				Alerts: []string{"slack", "invalid", "email"},
			},
			want:    ``,
			wantErr: true,
		},
		{
			name: "Test single invalid service",
			rule: chronograf.AlertRule{
				Alerts: []string{"invalid"},
			},
			want:    ``,
			wantErr: true,
		},
		{
			name: "Test single valid service",
			rule: chronograf.AlertRule{
				Alerts: []string{"slack"},
			},
			want: `alert()
        .slack()
`,
		},
	}
	for _, tt := range tests {
		got, err := AlertServices(tt.rule)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. AlertServices() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if tt.wantErr {
			continue
		}
		formatted, err := formatTick("alert()" + got)
		if err != nil {
			t.Errorf("%q. formatTick() error = %v", tt.name, err)
			continue
		}
		if formatted != tt.want {
			t.Errorf("%q. AlertServices() = %v, want %v", tt.name, formatted, tt.want)
		}
	}
}
