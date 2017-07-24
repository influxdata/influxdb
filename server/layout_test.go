package server_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/server"
)

func Test_Layouts(t *testing.T) {
	layoutTests := []struct {
		name       string
		expected   chronograf.Layout
		allLayouts []chronograf.Layout
		focusedApp string // should filter all layouts to this app only
		shouldErr  bool
	}{
		{
			"empty layout",
			chronograf.Layout{},
			[]chronograf.Layout{},
			"",
			false,
		},
		{
			"several layouts",
			chronograf.Layout{
				ID:          "d20a21c8-69f1-4780-90fe-e69f5e4d138c",
				Application: "influxdb",
				Measurement: "influxdb",
			},
			[]chronograf.Layout{
				chronograf.Layout{
					ID:          "d20a21c8-69f1-4780-90fe-e69f5e4d138c",
					Application: "influxdb",
					Measurement: "influxdb",
				},
			},
			"",
			false,
		},
		{
			"filtered app",
			chronograf.Layout{
				ID:          "d20a21c8-69f1-4780-90fe-e69f5e4d138c",
				Application: "influxdb",
				Measurement: "influxdb",
			},
			[]chronograf.Layout{
				chronograf.Layout{
					ID:          "d20a21c8-69f1-4780-90fe-e69f5e4d138c",
					Application: "influxdb",
					Measurement: "influxdb",
				},
				chronograf.Layout{
					ID:          "b020101b-ea6b-4c8c-9f0e-db0ba501f4ef",
					Application: "chronograf",
					Measurement: "chronograf",
				},
			},
			"influxdb",
			false,
		},
	}

	for _, test := range layoutTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// setup mock chronograf.Service and mock logger
			lg := &mocks.TestLogger{}
			svc := server.Service{
				LayoutStore: &mocks.LayoutStore{
					AllF: func(ctx context.Context) ([]chronograf.Layout, error) {
						if len(test.allLayouts) == 0 {
							return []chronograf.Layout{
								test.expected,
							}, nil
						} else {
							return test.allLayouts, nil
						}
					},
				},
				Logger: lg,
			}

			// setup mock request and response
			rr := httptest.NewRecorder()
			reqURL := url.URL{
				Path: "/chronograf/v1/layouts",
			}
			params := reqURL.Query()

			// add query params required by test
			if test.focusedApp != "" {
				params.Add("app", test.focusedApp)
			}

			// re-inject query params
			reqURL.RawQuery = params.Encode()

			req := httptest.NewRequest("GET", reqURL.RequestURI(), strings.NewReader(""))

			// invoke handler for layouts endpoint
			svc.Layouts(rr, req)

			// create a throwaway frame to unwrap Layouts
			respFrame := struct {
				Layouts []struct {
					chronograf.Layout
					Link interface{} `json:"-"`
				} `json:"layouts"`
			}{}

			// decode resp into respFrame
			resp := rr.Result()
			if err := json.NewDecoder(resp.Body).Decode(&respFrame); err != nil {
				t.Fatalf("%q - Error unmarshaling JSON: err: %s", test.name, err.Error())
			}

			// compare actual and expected
			if !cmp.Equal(test.expected, respFrame.Layouts[0].Layout) {
				t.Fatalf("%q - Expected layouts to be equal: diff:\n\t%s", test.name, cmp.Diff(test.expected, respFrame.Layouts[0].Layout))
			}
		})
	}
}
