package kapacitor

import (
	"log"
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
				AlertHandlers: chronograf.AlertHandlers{
					Slack:     []*chronograf.Slack{{}},
					VictorOps: []*chronograf.VictorOps{{}},
					Email:     []*chronograf.Email{{}},
				},
			},
			want: `alert()
        .email()
        .victorOps()
        .slack()
`,
		},
		{
			name: "Test single valid service",
			rule: chronograf.AlertRule{
				AlertHandlers: chronograf.AlertHandlers{
					Slack: []*chronograf.Slack{{}},
				},
			},
			want: `alert()
        .slack()
`,
		},
		{
			name: "Test single valid service and property",
			rule: chronograf.AlertRule{
				AlertHandlers: chronograf.AlertHandlers{
					Slack: []*chronograf.Slack{
						{
							Channel: "#general",
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
				AlertHandlers: chronograf.AlertHandlers{
					TCPs: []*chronograf.TCP{
						{
							Address: "myaddress:22",
						},
					},
				},
			},
			want: `alert()
        .tcp('myaddress:22')
`,
		},
		{
			name: "Test log",
			rule: chronograf.AlertRule{
				AlertHandlers: chronograf.AlertHandlers{
					Log: []*chronograf.Log{
						{
							FilePath: "/tmp/alerts.log",
						},
					},
				},
			},
			want: `alert()
        .log('/tmp/alerts.log')
`,
		},
		{
			name: "Test http as post",
			rule: chronograf.AlertRule{
				AlertHandlers: chronograf.AlertHandlers{
					Posts: []*chronograf.Post{
						{
							URL: "http://myaddress",
						},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
`,
		},
		{
			name: "Test post with headers",
			rule: chronograf.AlertRule{
				AlertHandlers: chronograf.AlertHandlers{
					Posts: []*chronograf.Post{
						{
							URL:     "http://myaddress",
							Headers: map[string]string{"key": "value"},
						},
					},
				},
			},
			want: `alert()
        .post('http://myaddress')
        .header('key', 'value')
`,
		},
	}
	for _, tt := range tests {
		got, err := AlertServices(tt.rule)
		if (err != nil) != tt.wantErr {
			log.Printf("GOT %s", got)
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

func Test_addAlertNodes(t *testing.T) {
	tests := []struct {
		name     string
		handlers chronograf.AlertHandlers
		want     string
		wantErr  bool
	}{
		{
			name: "foo",
			handlers: chronograf.AlertHandlers{
				IsStateChangesOnly: true,
				Email: []*chronograf.Email{
					{
						To: []string{
							"me@me.com", "you@you.com",
						},
					},
				},
			},
			want: `
        .stateChangesOnly()
        .email()
        .to('me@me.com')
        .to('you@you.com')
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addAlertNodes(tt.handlers)
			if (err != nil) != tt.wantErr {
				t.Errorf("addAlertNodes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("addAlertNodes() =\n%v\n, want\n%v", got, tt.want)
			}
		})
	}
}
