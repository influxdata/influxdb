package models

import (
	"reflect"
	"testing"
	"time"
)

func TestCheckTimeOutOfRangeErrorByPrecision(t *testing.T) {
	type args struct {
		timestamp int64
		precision string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "nanoSeconds", args: args{timestamp: 1554210031000000000, precision: "ns"}, want: time.Unix(0, 1554210031000000000).UTC().String()},
		{name: "microSeconds", args: args{timestamp: 1554210031000000000, precision: "us"}, want: "time 1554210031000000000 is outside the range [-9223372036854775, 9223372036854775]", wantErr: true},
		{name: "milliSeconds", args: args{timestamp: 1554210031000000000, precision: "ms"}, want: "time 1554210031000000000 is outside the range [-9223372036854, 9223372036854]", wantErr: true},
		{name: "seconds", args: args{timestamp: 1554210031000000000, precision: "s"}, want: "time 1554210031000000000 is outside the range [-9223372036, 9223372036]", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeCalcTime(tt.args.timestamp, tt.args.precision)
			if tt.wantErr {
				if !reflect.DeepEqual(err.Error(), tt.want) {
					t.Errorf("SafeCalcTime() = %v, want %v", err.Error(), tt.want)
				}
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want) {
				t.Errorf("SafeCalcTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
