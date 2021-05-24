package internal_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/chronograf"
	"github.com/influxdata/influxdb/v2/chronograf/bolt/internal"
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
		ID:                 12,
		SrcID:              2,
		Name:               "Fountain of Truth",
		Username:           "docbrown",
		Password:           "1 point twenty-one g1g@w@tts",
		URL:                "http://oldmanpeabody.mall.io:9092",
		InsecureSkipVerify: true,
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
						Label:  "foo",
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
						Source: "/chronograf/v1/sources/1",
						Shifts: []chronograf.TimeShift{},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{"0", "3", "1-7", "foo"},
						Label:  "foo",
						Prefix: "M",
						Suffix: "m",
						Base:   "2",
						Scale:  "roflscale",
					},
				},
				Type: "line",
				CellColors: []chronograf.CellColor{
					{
						ID:    "myid",
						Type:  "min",
						Hex:   "#234567",
						Name:  "Laser",
						Value: "0",
					},
					{
						ID:    "id2",
						Type:  "max",
						Hex:   "#876543",
						Name:  "Solitude",
						Value: "100",
					},
				},
				TableOptions: chronograf.TableOptions{},
				FieldOptions: []chronograf.RenamableField{},
				TimeFormat:   "",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshalling dashboard: err:", err)
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
						Shifts: []chronograf.TimeShift{},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						LegacyBounds: [2]int64{0, 5},
					},
				},
				CellColors: []chronograf.CellColor{
					{
						ID:    "myid",
						Type:  "min",
						Hex:   "#234567",
						Name:  "Laser",
						Value: "0",
					},
					{
						ID:    "id2",
						Type:  "max",
						Hex:   "#876543",
						Name:  "Solitude",
						Value: "100",
					},
				},
				TableOptions: chronograf.TableOptions{},
				TimeFormat:   "MM:DD:YYYY",
				FieldOptions: []chronograf.RenamableField{},
				Type:         "line",
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
						Shifts: []chronograf.TimeShift{},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{},
						Base:   "10",
						Scale:  "linear",
					},
				},
				CellColors: []chronograf.CellColor{
					{
						ID:    "myid",
						Type:  "min",
						Hex:   "#234567",
						Name:  "Laser",
						Value: "0",
					},
					{
						ID:    "id2",
						Type:  "max",
						Hex:   "#876543",
						Name:  "Solitude",
						Value: "100",
					},
				},
				TableOptions: chronograf.TableOptions{},
				FieldOptions: []chronograf.RenamableField{},
				TimeFormat:   "MM:DD:YYYY",
				Type:         "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshalling dashboard: err:", err)
	} else if !cmp.Equal(expected, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(expected, actual))
	}
}

func Test_MarshalDashboard_WithEmptyLegacyBounds(t *testing.T) {
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
						Shifts: []chronograf.TimeShift{},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						LegacyBounds: [2]int64{},
					},
				},
				CellColors: []chronograf.CellColor{
					{
						ID:    "myid",
						Type:  "min",
						Hex:   "#234567",
						Name:  "Laser",
						Value: "0",
					},
					{
						ID:    "id2",
						Type:  "max",
						Hex:   "#876543",
						Name:  "Solitude",
						Value: "100",
					},
				},
				Type:         "line",
				TableOptions: chronograf.TableOptions{},
				FieldOptions: []chronograf.RenamableField{},
				TimeFormat:   "MM:DD:YYYY",
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
						Shifts: []chronograf.TimeShift{},
					},
				},
				Axes: map[string]chronograf.Axis{
					"y": chronograf.Axis{
						Bounds: []string{},
						Base:   "10",
						Scale:  "linear",
					},
				},
				CellColors: []chronograf.CellColor{
					{
						ID:    "myid",
						Type:  "min",
						Hex:   "#234567",
						Name:  "Laser",
						Value: "0",
					},
					{
						ID:    "id2",
						Type:  "max",
						Hex:   "#876543",
						Name:  "Solitude",
						Value: "100",
					},
				},
				TableOptions: chronograf.TableOptions{},
				FieldOptions: []chronograf.RenamableField{},
				TimeFormat:   "MM:DD:YYYY",
				Type:         "line",
			},
		},
		Templates: []chronograf.Template{},
		Name:      "Dashboard",
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshalling dashboard: err:", err)
	} else if !cmp.Equal(expected, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(expected, actual))
	}
}

func Test_MarshalDashboard_WithEmptyCellType(t *testing.T) {
	dashboard := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID: "9b5367de-c552-4322-a9e8-7f384cbd235c",
			},
		},
	}

	expected := chronograf.Dashboard{
		ID: 1,
		Cells: []chronograf.DashboardCell{
			{
				ID:           "9b5367de-c552-4322-a9e8-7f384cbd235c",
				Type:         "line",
				Queries:      []chronograf.DashboardQuery{},
				Axes:         map[string]chronograf.Axis{},
				CellColors:   []chronograf.CellColor{},
				TableOptions: chronograf.TableOptions{},
				FieldOptions: []chronograf.RenamableField{},
			},
		},
		Templates: []chronograf.Template{},
	}

	var actual chronograf.Dashboard
	if buf, err := internal.MarshalDashboard(dashboard); err != nil {
		t.Fatal("Error marshaling dashboard: err", err)
	} else if err := internal.UnmarshalDashboard(buf, &actual); err != nil {
		t.Fatal("Error unmarshalling dashboard: err:", err)
	} else if !cmp.Equal(expected, actual) {
		t.Fatalf("Dashboard protobuf copy error: diff follows:\n%s", cmp.Diff(expected, actual))
	}
}
