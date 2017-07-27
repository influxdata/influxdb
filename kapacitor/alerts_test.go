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
		{
			name: "Test single valid service and property",
			rule: chronograf.AlertRule{
				Alerts: []string{"slack"},
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "slack",
						Properties: []chronograf.KapacitorProperty{
							{
								Name: "channel",
								Args: []string{"#general"},
							},
						},
					},
				},
			},
			want: `alert()
        .slack()
        .channel('#general')
`,
		},
		{
			name: "Test tcp",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "tcp",
						Args: []string{"myaddress:22"},
					},
				},
			},
			want: `alert()
        .tcp('myaddress:22')
`,
		},
		{
			name: "Test tcp no argument",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "tcp",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test log",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "log",
						Args: []string{"/tmp/alerts.log"},
					},
				},
			},
			want: `alert()
        .log('/tmp/alerts.log')
`,
		},
		{
			name: "Test log no argument",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "log",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test tcp no argument with other services",
			rule: chronograf.AlertRule{
				Alerts: []string{"slack", "tcp", "email"},
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "tcp",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test http as post",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "http",
						Args: []string{"http://myaddress"},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
`,
		},
		{
			name: "Test post",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "post",
						Args: []string{"http://myaddress"},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
`,
		},
		{
			name: "Test http no arguments",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "http",
					},
				},
			},
			want: `alert()
        .post()
`,
		},
		{
			name: "Test post with headers",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "post",
						Args: []string{"http://myaddress"},
						Properties: []chronograf.KapacitorProperty{
							{
								Name: "header",
								Args: []string{"key", "value"},
							},
						},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
        .header('key', 'value')
`,
		},
		{
			name: "Test post with headers",
			rule: chronograf.AlertRule{
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "post",
						Args: []string{"http://myaddress"},
						Properties: []chronograf.KapacitorProperty{
							{
								Name: "endpoint",
								Args: []string{"myendpoint"},
							},
						},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
        .endpoint('myendpoint')
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
