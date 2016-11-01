package tickscripts

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestValidateAlert(t *testing.T) {
	tests := []struct {
		name    string
		alert   Alert
		wantErr bool
	}{
		{
			name: "Test valid template alert",
			alert: Alert{
				Service: "slack",
			},
			wantErr: false,
		},
		{
			name: "Test invalid template alert",
			alert: Alert{
				Service: "invalid",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		if err := ValidateAlert(&tt.alert); (err != nil) != tt.wantErr {
			t.Errorf("%q. ValidateAlert() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func Test_validateTick(t *testing.T) {
	type args struct {
		script string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Valid Script",
			args: args{
				script: "stream|from()",
			},
			wantErr: false,
		},
		{
			name: "Invalid Script",
			args: args{
				script: "stream|nothing",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		if err := validateTick(tt.args.script); (err != nil) != tt.wantErr {
			t.Errorf("%q. validateTick() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestThreshold(t *testing.T) {
	tests := []struct {
		name    string
		alert   Alert
		want    chronograf.TickTemplate
		wantErr bool
	}{
		{
			name: "Test valid template alert",
			alert: Alert{
				Service:  "slack",
				Operator: ">",
			},
			want: `var database = 'telegraf'

var rp = 'autogen'

var measurement string

var metric string

var groupby = ['host']

var crit int

var period duration

var every duration

var message string

var id string

stream
    |from()
        .database(database)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)
    |window()
        .period(period)
        .every(every)
    |mean(metric)
        .as('stat')
    |alert()
        .id(id)
        .message(message)
        .crit(lambda: "stat" > crit)
        .slack()
`,
			wantErr: false,
		},
		{
			name: "Test valid template alert",
			alert: Alert{
				Service:  "invalid",
				Operator: ">",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := tt.alert.Threshold()
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Threshold() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. Threshold() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestRelative(t *testing.T) {
	tests := []struct {
		name    string
		alert   Alert
		want    chronograf.TickTemplate
		wantErr bool
	}{
		{
			name: "Test valid template alert",
			alert: Alert{
				Service:   "slack",
				Operator:  ">",
				Aggregate: "mean",
			},
			want: `var database = 'telegraf'

var rp = 'autogen'

var measurement string

var metric string

var groupby = ['host']

var crit int

var period duration

var every duration

var shift duration

var message string

var id string

var data = stream
    |from()
        .database(database)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)

var past = data
    |window()
        .period(period)
        .every(every)
        .align()
    |mean(metric)
        .as('stat')
    |shift(shift)

var current = data
    |window()
        .period(period)
        .every(every)
        .align()
    |mean(metric)
        .as('stat')

past
    |join(current)
        .as('past', 'current')
    |eval(lambda: abs(float("current.stat" - "past.stat")) / float("past.stat"))
        .keep()
        .as('perc')
    |alert()
        .id(id)
        .message(message)
        .crit(lambda: "perc" > crit)
        .slack()
`,
			wantErr: false,
		},
		{
			name: "Test invalid service template",
			alert: Alert{
				Service:   "invalid",
				Operator:  ">",
				Aggregate: "mean",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Test invalid aggregate template",
			alert: Alert{
				Service:   "slack",
				Operator:  ">",
				Aggregate: "invalid",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Test invalid operator template",
			alert: Alert{
				Service:   "slack",
				Operator:  "invalid",
				Aggregate: "mean",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := tt.alert.Relative()
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Relative() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. Relative() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDeadman(t *testing.T) {
	tests := []struct {
		name    string
		alert   Alert
		want    chronograf.TickTemplate
		wantErr bool
	}{
		{
			name: "Test valid template alert",
			alert: Alert{
				Service: "slack",
			},
			want: `var database = 'telegraf'

var rp = 'autogen'

var measurement string

var groupby = ['host']

var threshold float

var period duration

var id string

var message string

stream
    |from()
        .database(database)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)
    |deadman(threshold, period)
        .id(id)
        .message(message)
        .slack()
`,
			wantErr: false,
		},
		{
			name: "Test valid template alert",
			alert: Alert{
				Service: "invalid",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := tt.alert.Deadman()
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Deadman() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. Deadman() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
