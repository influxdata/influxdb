package influx

import (
	"testing"
	"time"

	"github.com/influxdata/chronograf"
)

func Test_toLineProtocol(t *testing.T) {
	tests := []struct {
		name    string
		point   *chronograf.Point
		want    string
		wantErr bool
	}{
		0: {
			name:    "requires a measurement",
			point:   &chronograf.Point{},
			wantErr: true,
		},
		1: {
			name: "requires at least one field",
			point: &chronograf.Point{
				Measurement: "telegraf",
			},
			wantErr: true,
		},
		2: {
			name: "no tags produces line protocol",
			point: &chronograf.Point{
				Measurement: "telegraf",
				Fields: map[string]interface{}{
					"myfield": 1,
				},
			},
			want: "telegraf myfield=1i",
		},
		3: {
			name: "test all influx data types",
			point: &chronograf.Point{
				Measurement: "telegraf",
				Fields: map[string]interface{}{
					"int":          19,
					"uint":         uint(85),
					"float":        88.0,
					"string":       "mph",
					"time_machine": true,
					"invalidField": time.Time{},
				},
			},
			want: `telegraf float=88.000000,int=19i,string="mph",time_machine=true,uint=85u`,
		},
		4: {
			name: "test all influx data types",
			point: &chronograf.Point{
				Measurement: "telegraf",
				Tags: map[string]string{
					"marty": "mcfly",
					"doc":   "brown",
				},
				Fields: map[string]interface{}{
					"int":          19,
					"uint":         uint(85),
					"float":        88.0,
					"string":       "mph",
					"time_machine": true,
					"invalidField": time.Time{},
				},
				Time: 497115501000000000,
			},
			want: `telegraf,doc=brown,marty=mcfly float=88.000000,int=19i,string="mph",time_machine=true,uint=85u 497115501000000000`,
		},
		5: {
			name: "measurements with comma or spaces are escaped",
			point: &chronograf.Point{
				Measurement: "O Romeo, Romeo, wherefore art thou Romeo",
				Tags: map[string]string{
					"part": "JULIET",
				},
				Fields: map[string]interface{}{
					"act":   2,
					"scene": 2,
					"page":  2,
					"line":  33,
				},
			},
			want: `O\ Romeo\,\ Romeo\,\ wherefore\ art\ thou\ Romeo,part=JULIET act=2i,line=33i,page=2i,scene=2i`,
		},
		6: {
			name: "tags with comma, quota, space, equal are escaped",
			point: &chronograf.Point{
				Measurement: "quotes",
				Tags: map[string]string{
					"comma,": "comma,",
					`quote"`: `quote"`,
					"space ": `space "`,
					"equal=": "equal=",
				},
				Fields: map[string]interface{}{
					"myfield": 1,
				},
			},
			want: `quotes,comma\,=comma\,,equal\==equal\=,quote\"=quote\",space\ =space\ \" myfield=1i`,
		},
		7: {
			name: "fields with quotes or backslashes are escaped",
			point: &chronograf.Point{
				Measurement: "quotes",
				Fields: map[string]interface{}{
					`quote"\`: `quote"\`,
				},
			},
			want: `quotes quote\"\="quote\"\\"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toLineProtocol(tt.point)
			if (err != nil) != tt.wantErr {
				t.Errorf("toLineProtocol() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("toLineProtocol() = %v, want %v", got, tt.want)
			}
		})
	}
}
