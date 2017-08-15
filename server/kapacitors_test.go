package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/server"
)

const tickScript = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.crit(lambda: "usage_idle" < 10)
		.log('/tmp/alert')
`

func TestValidRuleRequest(t *testing.T) {
	tests := []struct {
		name    string
		rule    chronograf.AlertRule
		wantErr bool
	}{
		{
			name: "No every with functions",
			rule: chronograf.AlertRule{
				Query: &chronograf.QueryConfig{
					Fields: []chronograf.Field{
						{
							Field: "oldmanpeabody",
							Funcs: []string{"max"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "With every",
			rule: chronograf.AlertRule{
				Every: "10s",
				Query: &chronograf.QueryConfig{
					Fields: []chronograf.Field{
						{
							Field: "oldmanpeabody",
							Funcs: []string{"max"},
						},
					},
				},
			},
		},
		{
			name:    "No query config",
			rule:    chronograf.AlertRule{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := server.ValidRuleRequest(tt.rule); (err != nil) != tt.wantErr {
				t.Errorf("ValidRuleRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_KapacitorRulesGet(t *testing.T) {
	kapaTests := []struct {
		name        string
		requestPath string
		mockAlerts  []chronograf.AlertRule
		expected    []chronograf.AlertRule
	}{
		{
			"basic",
			"/chronograf/v1/sources/1/kapacitors/1/rules",
			[]chronograf.AlertRule{
				{
					ID:   "cpu_alert",
					Name: "cpu_alert",
				},
			},
			[]chronograf.AlertRule{
				{
					ID:     "cpu_alert",
					Name:   "cpu_alert",
					Alerts: []string{},
				},
			},
		},
	}

	for _, test := range kapaTests {
		test := test // needed to avoid data race
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// setup mock kapa API
			kapaSrv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				tsks := []map[string]interface{}{}
				for _, task := range test.mockAlerts {
					tsks = append(tsks, map[string]interface{}{
						"id":     task.ID,
						"script": tickScript,
						"status": "enabled",
						"link": map[string]interface{}{
							"rel":  "self",
							"href": "/kapacitor/v1/tasks/cpu_alert",
						},
					})
				}

				tasks := map[string]interface{}{
					"tasks": tsks,
				}

				err := json.NewEncoder(rw).Encode(&tasks)
				if err != nil {
					t.Error("Failed to encode JSON. err:", err)
				}
			}))
			defer kapaSrv.Close()

			// setup mock service and test logger
			testLogger := mocks.TestLogger{}
			svc := &server.Service{
				ServersStore: &mocks.ServersStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Server, error) {
						return chronograf.Server{
							SrcID: ID,
							URL:   kapaSrv.URL,
						}, nil
					},
				},
				Logger: &testLogger,
			}

			// setup request and response recorder
			req := httptest.NewRequest("GET", test.requestPath, strings.NewReader(""))
			rr := httptest.NewRecorder()

			// setup context and request params
			bg := context.Background()
			params := httprouter.Params{
				{
					"id",
					"1",
				},
				{
					"kid",
					"1",
				},
			}
			ctx := httprouter.WithParams(bg, params)
			req = req.WithContext(ctx)

			// invoke KapacitorRulesGet endpoint
			svc.KapacitorRulesGet(rr, req)

			// destructure response
			frame := struct {
				Rules []struct {
					chronograf.AlertRule
					TICKScript json.RawMessage `json:"tickscript"`
					Status     json.RawMessage `json:"status"`
					Links      json.RawMessage `json:"links"`
				} `json:"rules"`
			}{}

			resp := rr.Result()

			err := json.NewDecoder(resp.Body).Decode(&frame)
			if err != nil {
				t.Fatal("Err decoding kapa rule response: err:", err)
			}

			actual := make([]chronograf.AlertRule, len(frame.Rules))

			for idx, _ := range frame.Rules {
				actual[idx] = frame.Rules[idx].AlertRule
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatal("Expected HTTP 200 OK but got", resp.Status)
			}

			if !cmp.Equal(test.expected, actual) {
				t.Fatalf("%q - Alert rules differ! diff:\n%s\n", test.name, cmp.Diff(test.expected, actual))
			}
		})
	}
}
