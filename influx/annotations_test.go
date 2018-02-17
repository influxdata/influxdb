package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
)

func Test_toPoint(t *testing.T) {
	tests := []struct {
		name string
		anno *chronograf.Annotation
		now  time.Time
		want *chronograf.Point
	}{
		0: {
			name: "convert annotation to point w/o start and end times",
			anno: &chronograf.Annotation{
				ID:   "1",
				Text: "mytext",
				Type: "mytype",
			},
			now: time.Unix(0, 0),
			want: &chronograf.Point{
				Database:        DefaultDB,
				RetentionPolicy: DefaultRP,
				Measurement:     DefaultMeasurement,
				Time:            time.Time{}.UnixNano(),
				Tags: map[string]string{
					"id": "1",
				},
				Fields: map[string]interface{}{
					"deleted":          false,
					"start_time":       time.Time{}.UnixNano(),
					"modified_time_ns": int64(time.Unix(0, 0).UnixNano()),
					"text":             "mytext",
					"type":             "mytype",
				},
			},
		},
		1: {
			name: "convert annotation to point with start/end time",
			anno: &chronograf.Annotation{
				ID:        "1",
				Text:      "mytext",
				Type:      "mytype",
				StartTime: time.Unix(100, 0),
				EndTime:   time.Unix(200, 0),
			},
			now: time.Unix(0, 0),
			want: &chronograf.Point{
				Database:        DefaultDB,
				RetentionPolicy: DefaultRP,
				Measurement:     DefaultMeasurement,
				Time:            time.Unix(200, 0).UnixNano(),
				Tags: map[string]string{
					"id": "1",
				},
				Fields: map[string]interface{}{
					"deleted":          false,
					"start_time":       time.Unix(100, 0).UnixNano(),
					"modified_time_ns": int64(time.Unix(0, 0).UnixNano()),
					"text":             "mytext",
					"type":             "mytype",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toPoint(tt.anno, tt.now); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toDeletedPoint(t *testing.T) {
	tests := []struct {
		name string
		anno *chronograf.Annotation
		now  time.Time
		want *chronograf.Point
	}{
		0: {
			name: "convert annotation to point w/o start and end times",
			anno: &chronograf.Annotation{
				ID:      "1",
				EndTime: time.Unix(0, 0),
			},
			now: time.Unix(0, 0),
			want: &chronograf.Point{
				Database:        DefaultDB,
				RetentionPolicy: DefaultRP,
				Measurement:     DefaultMeasurement,
				Time:            0,
				Tags: map[string]string{
					"id": "1",
				},
				Fields: map[string]interface{}{
					"deleted":          true,
					"start_time":       int64(0),
					"modified_time_ns": int64(0),
					"text":             "",
					"type":             "",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDeletedPoint(tt.anno, tt.now); !cmp.Equal(got, tt.want) {
				t.Errorf("toDeletedPoint() = %s", cmp.Diff(got, tt.want))
			}
		})
	}
}

func Test_value_Int64(t *testing.T) {
	tests := []struct {
		name    string
		v       value
		idx     int
		want    int64
		wantErr bool
	}{
		{
			name:    "index out of range returns error",
			idx:     1,
			wantErr: true,
		},
		{
			name: "converts a string to int64",
			v: value{
				json.Number("1"),
			},
			idx:  0,
			want: int64(1),
		},
		{
			name: "when not a json.Number, return error",
			v: value{
				"howdy",
			},
			idx:     0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.v.Int64(tt.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("value.Int64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("value.Int64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_value_Time(t *testing.T) {
	tests := []struct {
		name    string
		v       value
		idx     int
		want    time.Time
		wantErr bool
	}{
		{
			name:    "index out of range returns error",
			idx:     1,
			wantErr: true,
		},
		{
			name: "converts a string to int64",
			v: value{
				json.Number("1"),
			},
			idx:  0,
			want: time.Unix(0, 1),
		},
		{
			name: "when not a json.Number, return error",
			v: value{
				"howdy",
			},
			idx:     0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.v.Time(tt.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("value.Time() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("value.Time() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_value_String(t *testing.T) {
	tests := []struct {
		name    string
		v       value
		idx     int
		want    string
		wantErr bool
	}{
		{
			name:    "index out of range returns error",
			idx:     1,
			wantErr: true,
		},
		{
			name: "converts a string",
			v: value{
				"howdy",
			},
			idx:  0,
			want: "howdy",
		},
		{
			name: "when not a string, return error",
			v: value{
				0,
			},
			idx:     0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.v.String(tt.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("value.String() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("value.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnnotationStore_queryAnnotations(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
	}
	tests := []struct {
		name    string
		client  chronograf.TimeSeries
		args    args
		want    []chronograf.Annotation
		wantErr bool
	}{
		{
			name: "query error returns an error",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return nil, fmt.Errorf("error")
				},
			},
			wantErr: true,
		},
		{
			name: "response marshal error returns an error",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse("", fmt.Errorf("")), nil
				},
			},
			wantErr: true,
		},
		{
			name: "Bad JSON returns an error",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse(`{}`, nil), nil
				},
			},
			wantErr: true,
		},

		{
			name: "Incorrect fields returns error",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse(`[{
						"series": [
							{
								"name": "annotations",
								"columns": [
									"time",
									"deleted",
									"id",
									"modified_time_ns",
									"start_time",
									"text",
									"type"
								],
								"values": [
									[
										1516920117000000000,
										true,
										"4ba9f836-20e8-4b8e-af51-e1363edd7b6d",
										1517425994487495051,
										0,
										"",
										""
									]
								]
							}
						]
					}
				]}]`, nil), nil
				},
			},
			wantErr: true,
		},
		{
			name: "two annotation response",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse(`[
						{
							"series": [
								{
									"name": "annotations",
									"columns": [
										"time",
										"start_time",
										"modified_time_ns",
										"text",
										"type",
										"id"
									],
									"values": [
										[
											1516920177345000000,
											0,
											1516989242129417403,
											"mytext",
											"mytype",
											"ecf3a75d-f1c0-40e8-9790-902701467e92"
										],
										[
											1516920177345000000,
											0,
											1517425914433539296,
											"mytext2",
											"mytype2",
											"ea0aa94b-969a-4cd5-912a-5db61d502268"
										]
									]
								}
							]
						}
					]`, nil), nil
				},
			},
			want: []chronograf.Annotation{
				{
					EndTime:   time.Unix(0, 1516920177345000000),
					StartTime: time.Unix(0, 0),
					Text:      "mytext",
					Type:      "mytype",
					ID:        "ecf3a75d-f1c0-40e8-9790-902701467e92",
				},
				{
					EndTime:   time.Unix(0, 1516920177345000000),
					StartTime: time.Unix(0, 0),
					Text:      "mytext2",
					Type:      "mytype2",
					ID:        "ea0aa94b-969a-4cd5-912a-5db61d502268",
				},
			},
		},
		{
			name: "same id returns one",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse(`[
						{
							"series": [
								{
									"name": "annotations",
									"columns": [
										"time",
										"start_time",
										"modified_time_ns",
										"text",
										"type",
										"id"
									],
									"values": [
										[
											1516920177345000000,
											0,
											1516989242129417403,
											"mytext",
											"mytype",
											"ea0aa94b-969a-4cd5-912a-5db61d502268"
										],
										[
											1516920177345000000,
											0,
											1517425914433539296,
											"mytext2",
											"mytype2",
											"ea0aa94b-969a-4cd5-912a-5db61d502268"
										]
									]
								}
							]
						}
					]`, nil), nil
				},
			},
			want: []chronograf.Annotation{
				{
					EndTime:   time.Unix(0, 1516920177345000000),
					StartTime: time.Unix(0, 0),
					Text:      "mytext2",
					Type:      "mytype2",
					ID:        "ea0aa94b-969a-4cd5-912a-5db61d502268",
				},
			},
		},
		{
			name: "no responses returns empty array",
			client: &mocks.TimeSeries{
				QueryF: func(context.Context, chronograf.Query) (chronograf.Response, error) {
					return mocks.NewResponse(`[ { } ]`, nil), nil
				},
			},
			want: []chronograf.Annotation{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AnnotationStore{
				client: tt.client,
			}
			got, err := a.queryAnnotations(tt.args.ctx, tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("AnnotationStore.queryAnnotations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AnnotationStore.queryAnnotations() = %v, want %v", got, tt.want)
			}
		})
	}
}
