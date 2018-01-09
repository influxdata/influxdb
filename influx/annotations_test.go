package influx

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func Test_annotationLP(t *testing.T) {
	type args struct {
		measurement string
		a           *chronograf.Annotation
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "creates line protocol from annotation",
			args: args{
				measurement: "mymeasurement",
				a: &chronograf.Annotation{
					Name:     "myname",
					Type:     "mytype",
					Time:     1234,
					Duration: 5,
					Text:     "mytext",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := annotationLP(tt.args.measurement, tt.args.a); got != tt.want {
				t.Errorf("annotationLP() = %v, want %v", got, tt.want)
			}
		})
	}
}
