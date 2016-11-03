package kapacitor

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestTrigger(t *testing.T) {
	tests := []struct {
		name    string
		rule    chronograf.AlertRule
		want    string
		wantErr bool
	}{
		{
			name: "Test Deadman",
			rule: chronograf.AlertRule{
				Trigger:   "deadman",
				Operator:  ">",
				Aggregate: "mean",
			},
			want: `var trigger = data
    |deadman(threshold, every)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
        .messageField(messagefield)
        .durationField(durationfield)
`,
			wantErr: false,
		},
		{
			name: "Test Relative",
			rule: chronograf.AlertRule{
				Trigger:   "relative",
				Operator:  ">",
				Aggregate: "mean",
			},
			want: `var past = data
    |mean(metric)
        .as('stat')
    |shift(shift)

var current = data
    |mean(metric)
        .as('stat')

var trigger = past
    |join(current)
        .as('past', 'current')
    |eval(lambda: abs(float("current.stat" - "past.stat")) / float("past.stat"))
        .keep()
        .as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
        .messageField(messagefield)
        .durationField(durationfield)
`,
			wantErr: false,
		},
		{
			name: "Test Threshold",
			rule: chronograf.AlertRule{
				Trigger:   "threshold",
				Operator:  ">",
				Aggregate: "median",
			},
			want: `var trigger = data
    |median(metric)
        .as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
        .messageField(messagefield)
        .durationField(durationfield)
`,
			wantErr: false,
		},
		{
			name: "Test Invalid",
			rule: chronograf.AlertRule{
				Type: "invalid",
			},
			want:    ``,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := Trigger(tt.rule)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Trigger() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		formatted, err := formatTick(got)
		if err != nil {
			t.Errorf("%q. formatTick() error = %v", tt.name, err)
			continue
		}
		if string(formatted) != tt.want {
			t.Errorf("%q. Trigger() = \n%v\n want \n%v\n", tt.name, string(formatted), tt.want)
		}
	}
}
