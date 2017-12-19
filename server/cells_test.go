package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
)

func Test_Cells_CorrectAxis(t *testing.T) {
	t.Parallel()

	axisTests := []struct {
		name       string
		cell       *chronograf.DashboardCell
		shouldFail bool
	}{
		{
			name: "correct axes",
			cell: &chronograf.DashboardCell{
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
		},
		{
			name: "invalid axes present",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"axis of evil": chronograf.Axis{
						Bounds: []string{"666", "666"},
					},
					"axis of awesome": chronograf.Axis{
						Bounds: []string{"1337", "31337"},
					},
				},
			},
			shouldFail: true,
		},
		{
			name: "linear scale value",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "linear",
						Bounds: []string{"0", "100"},
					},
				},
			},
		},
		{
			name: "log scale value",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "log",
						Bounds: []string{"0", "100"},
					},
				},
			},
		},
		{
			name: "invalid scale value",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Scale:  "potatoes",
						Bounds: []string{"0", "100"},
					},
				},
			},
			shouldFail: true,
		},
		{
			name: "base 10 axis",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "10",
						Bounds: []string{"0", "100"},
					},
				},
			},
		},
		{
			name: "base 2 axis",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "2",
						Bounds: []string{"0", "100"},
					},
				},
			},
		},
		{
			name: "invalid base",
			cell: &chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Base:   "all your base are belong to us",
						Bounds: []string{"0", "100"},
					},
				},
			},
			shouldFail: true,
		},
	}

	for _, test := range axisTests {
		t.Run(test.name, func(tt *testing.T) {
			if err := HasCorrectAxes(test.cell); err != nil && !test.shouldFail {
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
			name: "happy path",
			reqURL: &url.URL{
				Path: "/chronograf/v1/dashboards/1/cells",
			},
			ctxParams: map[string]string{
				"id": "1",
			},
			mockResponse: []chronograf.DashboardCell{},
			expected:     []chronograf.DashboardCell{},
			expectedCode: http.StatusOK,
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
					ID:         "3899be5a-f6eb-4347-b949-de2f4fbea859",
					X:          0,
					Y:          0,
					W:          4,
					H:          4,
					Name:       "CPU",
					Type:       "bar",
					Queries:    []chronograf.DashboardQuery{},
					CellColors: []chronograf.CellColor{},
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
			svc := &Service{
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

func TestHasCorrectColors(t *testing.T) {
	tests := []struct {
		name    string
		c       *chronograf.DashboardCell
		wantErr bool
	}{
		{
			name: "min type is valid",
			c: &chronograf.DashboardCell{
				CellColors: []chronograf.CellColor{
					{
						Type: "min",
						Hex:  "#FFFFFF",
					},
				},
			},
		},
		{
			name: "max type is valid",
			c: &chronograf.DashboardCell{
				CellColors: []chronograf.CellColor{
					{
						Type: "max",
						Hex:  "#FFFFFF",
					},
				},
			},
		},
		{
			name: "threshold type is valid",
			c: &chronograf.DashboardCell{
				CellColors: []chronograf.CellColor{
					{
						Type: "threshold",
						Hex:  "#FFFFFF",
					},
				},
			},
		},
		{
			name: "invalid color type",
			c: &chronograf.DashboardCell{
				CellColors: []chronograf.CellColor{
					{
						Type: "unknown",
						Hex:  "#FFFFFF",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid color hex",
			c: &chronograf.DashboardCell{
				CellColors: []chronograf.CellColor{
					{
						Type: "min",
						Hex:  "bad",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := HasCorrectColors(tt.c); (err != nil) != tt.wantErr {
				t.Errorf("HasCorrectColors() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestService_ReplaceDashboardCell(t *testing.T) {
	tests := []struct {
		name            string
		DashboardsStore chronograf.DashboardsStore
		ID              string
		CID             string
		w               *httptest.ResponseRecorder
		r               *http.Request
		want            string
	}{
		{
			name: "update cell retains query config",
			ID:   "1",
			CID:  "3c5c4102-fa40-4585-a8f9-917c77e37192",
			DashboardsStore: &mocks.DashboardsStore{
				UpdateF: func(ctx context.Context, target chronograf.Dashboard) error {
					return nil
				},
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{
						ID: ID,
						Cells: []chronograf.DashboardCell{
							{
								ID:   "3c5c4102-fa40-4585-a8f9-917c77e37192",
								W:    4,
								H:    4,
								Name: "Untitled Cell",
								Queries: []chronograf.DashboardQuery{
									{
										Command: "SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)",
										QueryConfig: chronograf.QueryConfig{
											ID:              "3cd3eaa4-a4b8-44b3-b69e-0c7bf6b91d9e",
											Database:        "telegraf",
											Measurement:     "cpu",
											RetentionPolicy: "autogen",
											Fields: []chronograf.Field{
												{
													Value: "mean",
													Type:  "func",
													Alias: "mean_usage_user",
													Args: []chronograf.Field{
														{
															Value: "usage_user",
															Type:  "field",
														},
													},
												},
											},
											Tags: map[string][]string{
												"cpu": {
													"ChristohersMBP2.lan",
												},
											},
											GroupBy: chronograf.GroupBy{
												Time: "2s",
												Tags: []string{},
											},
											AreTagsAccepted: true,
											Fill:            "null",
											RawText:         strPtr("SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)"),
											Range: &chronograf.DurationRange{
												Lower: "now() - 15m"},
											Shifts: []chronograf.TimeShift{},
										},
									},
								},
								Axes: map[string]chronograf.Axis{
									"x": {
										Bounds: []string{},
									},
									"y": {
										Bounds: []string{},
									},
									"y2": {
										Bounds: []string{},
									},
								},
								Type: "line",
								CellColors: []chronograf.CellColor{
									{
										ID:    "0",
										Type:  "min",
										Hex:   "#00C9FF",
										Name:  "laser",
										Value: "0",
									},
									{
										ID:    "1",
										Type:  "max",
										Hex:   "#9394FF",
										Name:  "comet",
										Value: "100",
									},
								},
							},
						},
					}, nil
				},
			},
			w: httptest.NewRecorder(),
			r: httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte(`
				{
					"i": "3c5c4102-fa40-4585-a8f9-917c77e37192",
					"x": 0,
					"y": 0,
					"w": 4,
					"h": 4,
					"name": "Untitled Cell",
					"queries": [
					  {
						"queryConfig": {
						  "id": "3cd3eaa4-a4b8-44b3-b69e-0c7bf6b91d9e",
						  "database": "telegraf",
						  "measurement": "cpu",
						  "retentionPolicy": "autogen",
						  "fields": [
							{
							  "value": "mean",
							  "type": "func",
							  "alias": "mean_usage_user",
							  "args": [{"value": "usage_user", "type": "field", "alias": ""}]
							}
						  ],
						  "tags": {"cpu": ["ChristohersMBP2.lan"]},
						  "groupBy": {"time": "2s", "tags": []},
						  "areTagsAccepted": true,
						  "fill": "null",
						  "rawText":
							"SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)",
						  "range": {"upper": "", "lower": "now() - 15m"},
						  "shifts": []
						},
						"query":
						  "SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)",
						"source": null
					  }
					],
					"axes": {
					  "x": {
						"bounds": [],
						"label": "",
						"prefix": "",
						"suffix": "",
						"base": "",
						"scale": ""
					  },
					  "y": {
						"bounds": [],
						"label": "",
						"prefix": "",
						"suffix": "",
						"base": "",
						"scale": ""
					  },
					  "y2": {
						"bounds": [],
						"label": "",
						"prefix": "",
						"suffix": "",
						"base": "",
						"scale": ""
					  }
					},
					"type": "line",
					"colors": [
					  {"type": "min", "hex": "#00C9FF", "id": "0", "name": "laser", "value": "0"},
					  {
						"type": "max",
						"hex": "#9394FF",
						"id": "1",
						"name": "comet",
						"value": "100"
					  }
					],
					"links": {
					  "self":
						"/chronograf/v1/dashboards/6/cells/3c5c4102-fa40-4585-a8f9-917c77e37192"
					}
				  }
				  `))),
			want: `{"i":"3c5c4102-fa40-4585-a8f9-917c77e37192","x":0,"y":0,"w":4,"h":4,"name":"Untitled Cell","queries":[{"query":"SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time \u003e :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)","queryConfig":{"id":"3cd3eaa4-a4b8-44b3-b69e-0c7bf6b91d9e","database":"telegraf","measurement":"cpu","retentionPolicy":"autogen","fields":[{"value":"mean","type":"func","alias":"mean_usage_user","args":[{"value":"usage_user","type":"field","alias":""}]}],"tags":{"cpu":["ChristohersMBP2.lan"]},"groupBy":{"time":"2s","tags":[]},"areTagsAccepted":true,"fill":"null","rawText":"SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time \u003e :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)","range":{"upper":"","lower":"now() - 15m"},"shifts":[]},"source":""}],"axes":{"x":{"bounds":[],"label":"","prefix":"","suffix":"","base":"","scale":""},"y":{"bounds":[],"label":"","prefix":"","suffix":"","base":"","scale":""},"y2":{"bounds":[],"label":"","prefix":"","suffix":"","base":"","scale":""}},"type":"line","colors":[{"id":"0","type":"min","hex":"#00C9FF","name":"laser","value":"0"},{"id":"1","type":"max","hex":"#9394FF","name":"comet","value":"100"}],"links":{"self":"/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192"}}
`,
		},
		{
			name: "dashboard doesn't exist",
			ID:   "1",
			DashboardsStore: &mocks.DashboardsStore{
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{}, fmt.Errorf("doesn't exist")
				},
			},
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("PUT", "/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192", nil),
			want: `{"code":404,"message":"ID 1 not found"}`,
		},
		{
			name: "cell doesn't exist",
			ID:   "1",
			CID:  "3c5c4102-fa40-4585-a8f9-917c77e37192",
			DashboardsStore: &mocks.DashboardsStore{
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{}, nil
				},
			},
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("PUT", "/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192", nil),
			want: `{"code":404,"message":"ID 3c5c4102-fa40-4585-a8f9-917c77e37192 not found"}`,
		},
		{
			name: "invalid query config",
			ID:   "1",
			CID:  "3c5c4102-fa40-4585-a8f9-917c77e37192",
			DashboardsStore: &mocks.DashboardsStore{
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{
						ID: ID,
						Cells: []chronograf.DashboardCell{
							{
								ID: "3c5c4102-fa40-4585-a8f9-917c77e37192",
							},
						},
					}, nil
				},
			},
			w: httptest.NewRecorder(),
			r: httptest.NewRequest("PUT", "/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192", bytes.NewReader([]byte(`{
					"i": "3c5c4102-fa40-4585-a8f9-917c77e37192",
					"x": 0,
					"y": 0,
					"w": 4,
					"h": 4,
					"name": "Untitled Cell",
					"queries": [
					  {
						"queryConfig": {
						  "fields": [
							{
							  "value": "invalid",
							  "type": "invalidType"
							}
						  ]
						}
					  }
					]
				  }`))),
			want: `{"code":422,"message":"invalid field type \"invalidType\" ; expect func, field, integer, number, regex, wildcard"}`,
		},
		{
			name: "JSON is not parsable",
			ID:   "1",
			CID:  "3c5c4102-fa40-4585-a8f9-917c77e37192",
			DashboardsStore: &mocks.DashboardsStore{
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{
						ID: ID,
						Cells: []chronograf.DashboardCell{
							{
								ID: "3c5c4102-fa40-4585-a8f9-917c77e37192",
							},
						},
					}, nil
				},
			},
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("PUT", "/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192", nil),
			want: `{"code":400,"message":"Unparsable JSON"}`,
		},
		{
			name: "not able to update store returns error message",
			ID:   "1",
			CID:  "3c5c4102-fa40-4585-a8f9-917c77e37192",
			DashboardsStore: &mocks.DashboardsStore{
				UpdateF: func(ctx context.Context, target chronograf.Dashboard) error {
					return fmt.Errorf("error")
				},
				GetF: func(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
					return chronograf.Dashboard{
						ID: ID,
						Cells: []chronograf.DashboardCell{
							{
								ID: "3c5c4102-fa40-4585-a8f9-917c77e37192",
							},
						},
					}, nil
				},
			},
			w: httptest.NewRecorder(),
			r: httptest.NewRequest("PUT", "/chronograf/v1/dashboards/1/cells/3c5c4102-fa40-4585-a8f9-917c77e37192", bytes.NewReader([]byte(`{
					"i": "3c5c4102-fa40-4585-a8f9-917c77e37192",
					"x": 0,
					"y": 0,
					"w": 4,
					"h": 4,
					"name": "Untitled Cell",
					"queries": [
					  {
						"queryConfig": {
						  "fields": [
							{
							  "value": "usage_user",
							  "type": "field"
							}
						  ]
						}
					  }
					]
				  }`))),
			want: `{"code":500,"message":"Error updating cell 3c5c4102-fa40-4585-a8f9-917c77e37192 in dashboard 1: error"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardsStore: tt.DashboardsStore,
				},
				Logger: &mocks.TestLogger{},
			}
			tt.r = WithContext(tt.r.Context(), tt.r, map[string]string{
				"id":  tt.ID,
				"cid": tt.CID,
			})
			s.ReplaceDashboardCell(tt.w, tt.r)
			got := tt.w.Body.String()
			if got != tt.want {
				t.Errorf("ReplaceDashboardCell() = got/want\n%s\n%s\n", got, tt.want)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}

func Test_newCellResponses(t *testing.T) {
	tests := []struct {
		name   string
		dID    chronograf.DashboardID
		dcells []chronograf.DashboardCell
		want   []dashboardCellResponse
	}{
		{
			name: "foo",
			dID:  chronograf.DashboardID(1),
			dcells: []chronograf.DashboardCell{
				chronograf.DashboardCell{
					ID:   "445f8dc0-4d73-4168-8477-f628690d18a3",
					X:    0,
					Y:    0,
					W:    4,
					H:    4,
					Name: "Untitled Cell",
					Queries: []chronograf.DashboardQuery{
						{
							Command: "SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)",
							Label:   "",
							QueryConfig: chronograf.QueryConfig{
								ID:              "8d5ec6da-13a5-423e-9026-7bc45649766c",
								Database:        "telegraf",
								Measurement:     "cpu",
								RetentionPolicy: "autogen",
								Fields: []chronograf.Field{
									{
										Value: "mean",
										Type:  "func",
										Alias: "mean_usage_user",
										Args: []chronograf.Field{
											{
												Value: "usage_user",
												Type:  "field",
												Alias: "",
											},
										},
									},
								},
								Tags: map[string][]string{"cpu": []string{"ChristohersMBP2.lan"}},
								GroupBy: chronograf.GroupBy{
									Time: "2s",
								},
								AreTagsAccepted: true,
								Fill:            "null",
								RawText:         strPtr("SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)"),
								Range: &chronograf.DurationRange{
									Lower: "now() - 15m",
								},
							},
							Source: "",
						},
					},
					Axes: map[string]chronograf.Axis{
						"x":  chronograf.Axis{},
						"y":  chronograf.Axis{},
						"y2": chronograf.Axis{},
					},
					Type: "line",
					CellColors: []chronograf.CellColor{
						chronograf.CellColor{ID: "0", Type: "min", Hex: "#00C9FF", Name: "laser", Value: "0"},
						chronograf.CellColor{ID: "1", Type: "max", Hex: "#9394FF", Name: "comet", Value: "100"},
					},
				},
			},
			want: []dashboardCellResponse{
				{
					DashboardCell: chronograf.DashboardCell{
						ID:   "445f8dc0-4d73-4168-8477-f628690d18a3",
						W:    4,
						H:    4,
						Name: "Untitled Cell",
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)",
								QueryConfig: chronograf.QueryConfig{
									ID:              "8d5ec6da-13a5-423e-9026-7bc45649766c",
									Database:        "telegraf",
									Measurement:     "cpu",
									RetentionPolicy: "autogen",
									Fields: []chronograf.Field{
										{
											Value: "mean",
											Type:  "func",
											Alias: "mean_usage_user",
											Args: []chronograf.Field{
												{
													Value: "usage_user",
													Type:  "field",
												},
											},
										},
									},
									Tags: map[string][]string{"cpu": {"ChristohersMBP2.lan"}},
									GroupBy: chronograf.GroupBy{
										Time: "2s",
									},
									AreTagsAccepted: true,
									Fill:            "null",
									RawText:         strPtr("SELECT mean(\"usage_user\") AS \"mean_usage_user\" FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > :dashboardTime: AND \"cpu\"=:cpu: GROUP BY :interval: FILL(null)"),
									Range: &chronograf.DurationRange{
										Lower: "now() - 15m",
									},
								},
							},
						},
						Axes: map[string]chronograf.Axis{
							"x":  {},
							"y":  {},
							"y2": {},
						},
						Type: "line",
						CellColors: []chronograf.CellColor{
							{
								ID:    "0",
								Type:  "min",
								Hex:   "#00C9FF",
								Name:  "laser",
								Value: "0",
							},
							{
								ID:    "1",
								Type:  "max",
								Hex:   "#9394FF",
								Name:  "comet",
								Value: "100",
							},
						},
					},
					Links: dashboardCellLinks{
						Self: "/chronograf/v1/dashboards/1/cells/445f8dc0-4d73-4168-8477-f628690d18a3"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCellResponses(tt.dID, tt.dcells); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCellResponses() = got-/want+ %s", cmp.Diff(got, tt.want))
			}
		})
	}
}
