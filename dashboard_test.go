package influxdb_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestView_MarshalJSON(t *testing.T) {
	type args struct {
		view platform.View
	}
	type wants struct {
		json string
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "xy",
			args: args{
				view: platform.View{
					ViewContents: platform.ViewContents{
						ID:   platformtesting.MustIDBase16("f01dab1ef005ba11"),
						Name: "XY widget",
					},
					Properties: platform.XYViewProperties{
						Type: "xy",
					},
				},
			},
			wants: wants{
				json: `
{
  "id": "f01dab1ef005ba11",
  "name": "XY widget",
  "properties": {
    "shape": "chronograf-v2",
    "queries": null,
    "axes": null,
    "type": "xy",
    "staticLegend": {},
    "geom": "",
    "colors": null,
    "note": "",
    "showNoteWhenEmpty": false,
    "xColumn": "",
    "generateXAxisTicks": null,
    "xTotalTicks": 0,
    "xTickStart": 0,
    "xTickStep": 0,
    "yColumn": "",
    "generateYAxisTicks": null,
    "yTotalTicks": 0,
    "yTickStart": 0,
    "yTickStep": 0,
    "shadeBelow": false,
    "position": "",
    "timeFormat": "",
    "hoverDimension": "",
    "legendColorizeRows": false,
    "legendOpacity": 0,
    "legendOrientationThreshold": 0
  }
}`,
			},
		},
		{
			name: "geo",
			args: args{
				view: platform.View{
					ViewContents: platform.ViewContents{
						ID:   platformtesting.MustIDBase16("e21da111ef005b11"),
						Name: "Circle Map",
					},
					Properties: platform.GeoViewProperties{
						Type:            platform.ViewPropertyTypeGeo,
						Zoom:            2,
						Center:          platform.Datum{Lat: 50.4, Lon: 10.1},
						AllowPanAndZoom: true,
						GeoLayers: []platform.GeoLayer{
							{
								Type:        "circleMap",
								RadiusField: "radius",
								ColorField:  "color",
								Radius:      12,
								Blur:        20,
								RadiusDimension: platform.Axis{
									Label:  "Frequency",
									Bounds: []string{"10", "20"},
									Suffix: "m",
								},
								ColorDimension: platform.Axis{
									Label:  "Severity",
									Bounds: []string{"10.0", "40"},
									Suffix: "%",
								},
								ViewColors: []platform.ViewColor{{
									Type: "min",
									Hex:  "#FF0000",
								}, {
									Hex:   "#000000",
									Value: 10,
								}, {
									Hex:   "#FFFFFF",
									Value: 40,
								}},
								IntensityDimension: platform.Axis{
									Label:  "Impact",
									Prefix: "$",
								},
								InterpolateColors: true,
								TrackWidth:        2,
								Speed:             1.0,
								IsClustered:       true,
							},
						},
						Note: "Some more information",
					},
				},
			},
			wants: wants{
				json: `
{
  "id": "e21da111ef005b11",
  "name": "Circle Map",
  "properties": {
    "shape": "chronograf-v2",
    "type": "geo",
    "queries": null,
    "center": {
      "lat": 50.4,
      "lon": 10.1
    },
    "zoom": 2,
    "mapStyle": "",
    "allowPanAndZoom": true,
    "detectCoordinateFields": false,
    "colors": null,
    "layers": [
      {
        "type": "circleMap",
        "radiusField": "radius",
        "colorField": "color",
        "intensityField": "",
        "colors": [
          {
            "id": "",
            "type": "min",
            "hex": "#FF0000",
            "name": "",
            "value": 0
          },
          {
            "id": "",
            "type": "",
            "hex": "#000000",
            "name": "",
            "value": 10
          },
          {
            "id": "",
            "type": "",
            "hex": "#FFFFFF",
            "name": "",
            "value": 40
          }
        ],
        "radius": 12,
        "blur": 20,
        "radiusDimension": {
          "bounds": [
            "10",
            "20"
          ],
          "label": "Frequency",
          "prefix": "",
          "suffix": "m",
          "base": "",
          "scale": ""
        },
        "colorDimension": {
          "bounds": [
            "10.0",
            "40"
          ],
          "label": "Severity",
          "prefix": "",
          "suffix": "%",
          "base": "",
          "scale": ""
        },
        "intensityDimension": {
          "bounds": null,
          "label": "Impact",
          "prefix": "$",
          "suffix": "",
          "base": "",
          "scale": ""
        },
        "interpolateColors": true,
        "trackWidth": 2,
        "speed": 1,
        "randomColors": false,
        "isClustered": true
      }
    ],
    "note": "Some more information",
    "showNoteWhenEmpty": false
  }
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.MarshalIndent(tt.args.view, "", "  ")
			if err != nil {
				t.Fatalf("error marshalling json")
			}

			eq, err := jsonEqual(string(b), tt.wants.json)
			if err != nil {
				t.Fatalf("error marshalling json %v", err)
			}
			if !eq {
				t.Errorf("JSON did not match\nexpected:%s\ngot:\n%s\n", tt.wants.json, string(b))
			}
		})
	}
}

func jsonEqual(s1, s2 string) (eq bool, err error) {
	var o1, o2 interface{}

	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}
	return cmp.Equal(o1, o2), nil
}
