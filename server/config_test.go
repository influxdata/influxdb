package server

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
)

func TestConfig(t *testing.T) {
	type fields struct {
		ConfigStore chronograf.ConfigStore
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name: "Get global application configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						Auth: chronograf.AuthConfig{
							SuperAdminNewUsers: false,
						},
						LogViewerUI: chronograf.LogViewerUIConfig{
							Columns: []chronograf.LogViewerUIColumn{
								{
									Name:     "severity",
									Position: 0,
									Encoding: []chronograf.ColumnEncoding{
										{
											Type:  "color",
											Value: "emergency",
											Name:  "ruby",
										},
										{
											Type:  "color",
											Value: "info",
											Name:  "rainforest",
										},
										{
											Type:  "displayName",
											Value: "Log Severity",
										},
									},
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/config"},"auth":{"superAdminNewUsers":false},"logViewerUI":{"columns":[{"name":"severity","position":0,"encoding":[{"type":"color","value":"emergency","name":"ruby"},{"type":"color","value":"info","name":"rainforest"},{"type":"displayName","value":"Log Severity"}]}]}}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					ConfigStore: tt.fields.ConfigStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)

			s.Config(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Config() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Config() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Config() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestConfigSection(t *testing.T) {
	type fields struct {
		ConfigStore chronograf.ConfigStore
	}
	type args struct {
		section string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Get auth configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						Auth: chronograf.AuthConfig{
							SuperAdminNewUsers: false,
						},
					},
				},
			},
			args: args{
				section: "auth",
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"superAdminNewUsers": false, "links": {"self": "/chronograf/v1/config/auth"}}`,
			},
		},
		{
			name: "Get log viewer configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						LogViewerUI: chronograf.LogViewerUIConfig{
							Columns: []chronograf.LogViewerUIColumn{
								{
									Name:     "severity",
									Position: 0,
									Encoding: []chronograf.ColumnEncoding{
										{
											Type:  "color",
											Value: "emergency",
											Name:  "ruby",
										},
										{
											Type:  "color",
											Value: "info",
											Name:  "rainforest",
										},
										{
											Type:  "displayName",
											Value: "Log Severity",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				section: "logViewer",
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/config/logViewer"},"columns":[{"name":"severity","position":0,"encoding":[{"type":"color","value":"emergency","name":"ruby"},{"type":"color","value":"info","name":"rainforest"},{"type":"displayName","value":"Log Severity"}]}]}`,
			},
		},
		{
			name: "Get unknown configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						Auth: chronograf.AuthConfig{
							SuperAdminNewUsers: false,
						},
					},
				},
			},
			args: args{
				section: "unknown",
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"received unknown section \"unknown\""}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					ConfigStore: tt.fields.ConfigStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(httprouter.WithParams(
				r.Context(),
				httprouter.Params{
					{
						Key:   "section",
						Value: tt.args.section,
					},
				}))

			s.ConfigSection(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Config() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Config() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Config() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestReplaceConfigSection(t *testing.T) {
	type fields struct {
		ConfigStore chronograf.ConfigStore
	}
	type args struct {
		section string
		payload interface{} // expects JSON serializable struct
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Set auth configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						Auth: chronograf.AuthConfig{
							SuperAdminNewUsers: false,
						},
					},
				},
			},
			args: args{
				section: "auth",
				payload: chronograf.AuthConfig{
					SuperAdminNewUsers: true,
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"superAdminNewUsers": true, "links": {"self": "/chronograf/v1/config/auth"}}`,
			},
		},
		{
			name: "Set log viewer configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						LogViewerUI: chronograf.LogViewerUIConfig{
							Columns: []chronograf.LogViewerUIColumn{
								{
									Name:     "severity",
									Position: 0,
									Encoding: []chronograf.ColumnEncoding{
										{
											Type:  "color",
											Value: "info",
											Name:  "rainforest",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				section: "logViewer",
				payload: chronograf.LogViewerUIConfig{
					Columns: []chronograf.LogViewerUIColumn{
						{
							Name:     "severity",
							Position: 1,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "color",
									Value: "info",
									Name:  "pineapple",
								},
								{
									Type:  "color",
									Value: "emergency",
									Name:  "ruby",
								},
							},
						},
						{
							Name:     "messages",
							Position: 0,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "displayName",
									Value: "Log Messages",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/config/logViewer"},"columns":[{"name":"severity","position":1,"encoding":[{"type":"color","value":"info","name":"pineapple"},{"type":"color","value":"emergency","name":"ruby"}]},{"name":"messages","position":0,"encoding":[{"type":"displayName","value":"Log Messages"}]}]}`,
			},
		},
		{
			name: "Set unknown configuration",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						Auth: chronograf.AuthConfig{
							SuperAdminNewUsers: false,
						},
					},
				},
			},
			args: args{
				section: "unknown",
				payload: struct {
					Data string `json:"data"`
				}{
					Data: "stuff",
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"received unknown section \"unknown\""}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					ConfigStore: tt.fields.ConfigStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(httprouter.WithParams(
				r.Context(),
				httprouter.Params{
					{
						Key:   "section",
						Value: tt.args.section,
					},
				}))
			buf, _ := json.Marshal(tt.args.payload)
			r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.ReplaceConfigSection(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Config() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Config() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Config() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
