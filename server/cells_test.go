package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/server"
)

func Test_Cells_CorrectAxis(t *testing.T) {
	t.Parallel()

	axisTests := []struct {
		name       string
		cell       *chronograf.DashboardCell
		shouldFail bool
	}{
		{
			"correct axes",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
					"y": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
					"y2": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"invalid axes present",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"axis of evil": chronograf.Axis{
						Bounds: []string{"666", "666"},
					},
					"axis of awesome": chronograf.Axis{
						Bounds: []string{"1337", "31337"},
					},
				},
			},
			true,
		},
		{
			"linear scale value",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "linear",
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"log scale value",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "log",
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"invalid scale value",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "potatoes",
						Bounds: []string{"0", "100"},
					},
				},
			},
			true,
		},
		{
			"base 10 axis",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "10",
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"base 2 axis",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "2",
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"invalid base",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "all your base are belong to us",
						Bounds: []string{"0", "100"},
					},
				},
			},
			true,
		},
	}

	for _, test := range axisTests {
		t.Run(test.name, func(tt *testing.T) {
			if err := server.HasCorrectAxes(test.cell); err != nil && !test.shouldFail {
				t.Errorf("%q: Unexpected error: err: %s", test.name, err)
			} else if err == nil && test.shouldFail {
				t.Errorf("%q: Expected error and received none", test.name)
			}
		})
	}
}

func Test_Service_DashboardCells(t *testing.T) {
	cellsTests := []struct {
		name         string
		reqURL       *url.URL
		ctxParams    map[string]string
		mockResponse []chronograf.DashboardCell
		expected     []chronograf.DashboardCell
		expectedCode int
	}{
		{
			"happy path",
			&url.URL{
				Path: "/chronograf/v1/dashboards/1/cells",
			},
			map[string]string{
				"id": "1",
			},
			[]chronograf.DashboardCell{},
			[]chronograf.DashboardCell{},
			http.StatusOK,
		},
		{
			name: "cell axes should always be \"x\", \"y\", and \"y2\"",
			reqURL: &url.URL{
				Path: "/chronograf/v1/dashboards/1/cells",
			},
			ctxParams: map[string]string{
				"id": "1",
			},
			mockResponse: []chronograf.DashboardCell{
				{
					ID:      "3899be5a-f6eb-4347-b949-de2f4fbea859",
					X:       0,
					Y:       0,
					W:       4,
					H:       4,
					Name:    "CPU",
					Type:    "bar",
					Queries: []chronograf.DashboardQuery{},
					Axes:    map[string]chronograf.Axis{},
				},
			},
			expected: []chronograf.DashboardCell{
				{
					ID:      "3899be5a-f6eb-4347-b949-de2f4fbea859",
					X:       0,
					Y:       0,
					W:       4,
					H:       4,
					Name:    "CPU",
					Type:    "bar",
					Queries: []chronograf.DashboardQuery{},
					Axes: map[string]chronograf.Axis{
						"x": chronograf.Axis{
							Bounds: []string{},
						},
						"y": chronograf.Axis{
							Bounds: []string{},
						},
						"y2": chronograf.Axis{
							Bounds: []string{},
						},
					},
				},
			},
			expectedCode: http.StatusOK,
		},
	}

	for _, test := range cellsTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// setup context with params
			ctx := context.Background()
			params := httprouter.Params{}
			for k, v := range test.ctxParams {
				params = append(params, httprouter.Param{
					Key:   k,
					Value: v,
				})
			}
			ctx = httprouter.WithParams(ctx, params)

			// setup response recorder and request
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("GET", test.reqURL.RequestURI(), strings.NewReader("")).WithContext(ctx)

			// setup mock DashboardCells store and logger
			tlog := &mocks.TestLogger{}
			svc := &server.Service{
				Store: &mocks.Store{
					DashboardsStore: &mocks.DashboardsStore{
						GetF: func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
							return chronograf.Dashboard{
								ID:        chronograf.DashboardID(1),
								Cells:     test.mockResponse,
								Templates: []chronograf.Template{},
								Name:      "empty dashboard",
							}, nil
						},
					},
				},
				Logger: tlog,
			}

			// invoke DashboardCell handler
			svc.DashboardCells(rr, req)

			// setup frame to decode response into
			respFrame := []struct {
				chronograf.DashboardCell
				Links json.RawMessage `json:"links"` // ignore links
			}{}

			// decode response
			resp := rr.Result()

			if resp.StatusCode != test.expectedCode {
				tlog.Dump(t)
				t.Fatalf("%q - Status codes do not match. Want %d (%s), Got %d (%s)", test.name, test.expectedCode, http.StatusText(test.expectedCode), resp.StatusCode, http.StatusText(resp.StatusCode))
			}

			if err := json.NewDecoder(resp.Body).Decode(&respFrame); err != nil {
				t.Fatalf("%q - Error unmarshaling response body: err: %s", test.name, err)
			}

			// extract actual
			actual := []chronograf.DashboardCell{}
			for _, rsp := range respFrame {
				actual = append(actual, rsp.DashboardCell)
			}

			// compare actual and expected
			if !cmp.Equal(actual, test.expected) {
				t.Fatalf("%q - Dashboard Cells do not match: diff: %s", test.name, cmp.Diff(actual, test.expected))
			}
		})
	}
}
