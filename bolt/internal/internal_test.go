package internal_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

func TestMarshalSource(t *testing.T) {
	v := chronograf.Source{
		ID:       12,
		Name:     "Fountain of Truth",
		Type:     "influx",
		Username: "docbrown",
		Password: "1 point twenty-one g1g@w@tts",
		URL:      "http://twin-pines.mall.io:8086",
		MetaURL:  "http://twin-pines.meta.io:8086",
		Default:  true,
		Telegraf: "telegraf",
	}

	var vv chronograf.Source
	if buf, err := internal.MarshalSource(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalSource(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}

	// Test if the new insecureskipverify works
	v.InsecureSkipVerify = true
	if buf, err := internal.MarshalSource(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalSource(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}
}
func TestMarshalSourceWithSecret(t *testing.T) {
	v := chronograf.Source{
		ID:           12,
		Name:         "Fountain of Truth",
		Type:         "influx",
		Username:     "docbrown",
		SharedSecret: "hunter2s",
		URL:          "http://twin-pines.mall.io:8086",
		MetaURL:      "http://twin-pines.meta.io:8086",
		Default:      true,
		Telegraf:     "telegraf",
	}

	var vv chronograf.Source
	if buf, err := internal.MarshalSource(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalSource(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}

	// Test if the new insecureskipverify works
	v.InsecureSkipVerify = true
	if buf, err := internal.MarshalSource(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalSource(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}
}

func TestMarshalServer(t *testing.T) {
	v := chronograf.Server{
		ID:       12,
		SrcID:    2,
		Name:     "Fountain of Truth",
		Username: "docbrown",
		Password: "1 point twenty-one g1g@w@tts",
		URL:      "http://oldmanpeabody.mall.io:9092",
	}

	var vv chronograf.Server
	if buf, err := internal.MarshalServer(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalServer(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}
}

func TestMarshalLayout(t *testing.T) {
	layout := chronograf.Layout{
		ID:          "id",
		Measurement: "measurement",
		Application: "app",
		Cells: []chronograf.Cell{
			{
				X:    1,
				Y:    1,
				W:    4,
				H:    4,
				I:    "anotherid",
				Type: "line",
				Name: "cell1",
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
				},
				Queries: []chronograf.Query{
					{
						Range: &chronograf.Range{
							Lower: 1,
							Upper: 2,
						},
						Label:   "y1",
						Command: "select mean(usage_user) as usage_user from cpu",
						Wheres: []string{
							`"host"="myhost"`,
						},
						GroupBys: []string{
							`"cpu"`,
						},
					},
				},
			},
		},
	}

	var vv chronograf.Layout
	if buf, err := internal.MarshalLayout(layout); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalLayout(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !cmp.Equal(layout, vv) {
		t.Fatal("source protobuf copy error: diff:\n", cmp.Diff(layout, vv))
	}
}

func Test_MarshalDashboard(t *testing.T) {
	dashboard := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:   "9b5367de-c552-4322-a9e8-7f384cbd235c",
				X:    0,
				Y:    0,
				W:    4,
				H:    4,
				Name: "Super awesome query",
				Queries: []chronograf.DashboardQuery{
					{
						Command: "select * from cpu",
						Label:   "CPU Utilization",
						Range: &chronograf.Range{
							Upper: int64(100),
						},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{"0", "3", "1-7", "foo"},
						Label:  "foo",
					},
				},
				Type: "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshaling dashboard: err:", err)
	} else if !cmp.Equal(dashboard, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(dashboard, actual))
	}
}

func Test_MarshalDashboard_WithLegacyBounds(t *testing.T) {
	dashboard := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:   "9b5367de-c552-4322-a9e8-7f384cbd235c",
				X:    0,
				Y:    0,
				W:    4,
				H:    4,
				Name: "Super awesome query",
				Queries: []chronograf.DashboardQuery{
					{
						Command: "select * from cpu",
						Label:   "CPU Utilization",
						Range: &chronograf.Range{
							Upper: int64(100),
						},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						LegacyBounds: [2]int64{0, 5},
					},
				},
				Type: "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	expected := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:   "9b5367de-c552-4322-a9e8-7f384cbd235c",
				X:    0,
				Y:    0,
				W:    4,
				H:    4,
				Name: "Super awesome query",
				Queries: []chronograf.DashboardQuery{
					{
						Command: "select * from cpu",
						Label:   "CPU Utilization",
						Range: &chronograf.Range{
							Upper: int64(100),
						},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{},
					},
				},
				Type: "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshaling dashboard: err:", err)
	} else if !cmp.Equal(expected, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(expected, actual))
	}
}

func Test_MarshalDashboard_WithNoLegacyBounds(t *testing.T) {
	dashboard := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:   "9b5367de-c552-4322-a9e8-7f384cbd235c",
				X:    0,
				Y:    0,
				W:    4,
				H:    4,
				Name: "Super awesome query",
				Queries: []chronograf.DashboardQuery{
					{
						Command: "select * from cpu",
						Label:   "CPU Utilization",
						Range: &chronograf.Range{
							Upper: int64(100),
						},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						LegacyBounds: [2]int64{},
					},
				},
				Type: "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	expected := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:   "9b5367de-c552-4322-a9e8-7f384cbd235c",
				X:    0,
				Y:    0,
				W:    4,
				H:    4,
				Name: "Super awesome query",
				Queries: []chronograf.DashboardQuery{
					{
						Command: "select * from cpu",
						Label:   "CPU Utilization",
						Range: &chronograf.Range{
							Upper: int64(100),
						},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{},
					},
				},
				Type: "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshaling dashboard: err:", err)
	} else if !cmp.Equal(expected, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(expected, actual))
	}
}
