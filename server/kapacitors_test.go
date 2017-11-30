package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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
							Value: "max",
							Type:  "func",
							Args: []chronograf.Field{
								{
									Value: "oldmanpeabody",
									Type:  "field",
								},
							},
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
							Value: "max",
							Type:  "func",
							Args: []chronograf.Field{
								{
									Value: "oldmanpeabody",
									Type:  "field",
								},
							},
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
			name:        "basic",
			requestPath: "/chronograf/v1/sources/1/kapacitors/1/rules",
			mockAlerts: []chronograf.AlertRule{
				{
					ID:         "cpu_alert",
					Name:       "cpu_alert",
					Status:     "enabled",
					Type:       "stream",
					DBRPs:      []chronograf.DBRP{{DB: "telegraf", RP: "autogen"}},
					TICKScript: tickScript,
				},
			},
			expected: []chronograf.AlertRule{
				{
					ID:         "cpu_alert",
					Name:       "cpu_alert",
					Status:     "enabled",
					Type:       "stream",
					DBRPs:      []chronograf.DBRP{{DB: "telegraf", RP: "autogen"}},
					TICKScript: tickScript,
					AlertHandlers: chronograf.AlertHandlers{
						Posts:     []*chronograf.Post{},
						TCPs:      []*chronograf.TCP{},
						Email:     []*chronograf.Email{},
						Exec:      []*chronograf.Exec{},
						Log:       []*chronograf.Log{},
						VictorOps: []*chronograf.VictorOps{},
						PagerDuty: []*chronograf.PagerDuty{},
						Pushover:  []*chronograf.Pushover{},
						Sensu:     []*chronograf.Sensu{},
						Slack:     []*chronograf.Slack{},
						Telegram:  []*chronograf.Telegram{},
						HipChat:   []*chronograf.HipChat{},
						Alerta:    []*chronograf.Alerta{},
						OpsGenie:  []*chronograf.OpsGenie{},
						Talk:      []*chronograf.Talk{},
					},
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
				params := r.URL.Query()
				limit, err := strconv.Atoi(params.Get("limit"))
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				offset, err := strconv.Atoi(params.Get("offset"))
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}

				tsks := []map[string]interface{}{}
				for _, task := range test.mockAlerts {
					tsks = append(tsks, map[string]interface{}{
						"id":     task.ID,
						"script": tickScript,
						"status": "enabled",
						"type":   "stream",
						"dbrps": []chronograf.DBRP{
							{
								DB: "telegraf",
								RP: "autogen",
							},
						},
						"link": map[string]interface{}{
							"rel":  "self",
							"href": "/kapacitor/v1/tasks/cpu_alert",
						},
					})
				}

				var tasks map[string]interface{}

				if offset >= len(tsks) {
					tasks = map[string]interface{}{
						"tasks": []map[string]interface{}{},
					}
				} else if limit+offset > len(tsks) {
					tasks = map[string]interface{}{
						"tasks": tsks[offset:],
					}
				}
				//} else {
				//tasks = map[string]interface{}{
				//"tasks": tsks[offset : offset+limit],
				//}
				//}

				err = json.NewEncoder(rw).Encode(&tasks)
				if err != nil {
					t.Error("Failed to encode JSON. err:", err)
				}
			}))
			defer kapaSrv.Close()

			// setup mock service and test logger
			testLogger := mocks.TestLogger{}
			svc := &server.Service{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:                 ID,
							InsecureSkipVerify: true,
						}, nil
					},
				},
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
					Links json.RawMessage `json:"links"`
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
