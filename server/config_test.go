package server

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

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

func TestAuthConfig(t *testing.T) {
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
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"superAdminNewUsers": false, "links": {"self": "/chronograf/v1/config/auth"}}`,
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

			s.AuthConfig(w, r)

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

func TestReplaceAuthConfig(t *testing.T) {
	type fields struct {
		ConfigStore chronograf.ConfigStore
	}
	type args struct {
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
			buf, _ := json.Marshal(tt.args.payload)
			r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.ReplaceAuthConfig(w, r)

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

func TestLogViewerUIConfig(t *testing.T) {
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
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/config/logViewer"},"columns":[{"name":"severity","position":0,"encoding":[{"type":"color","value":"emergency","name":"ruby"},{"type":"color","value":"info","name":"rainforest"},{"type":"displayName","value":"Log Severity"}]}]}`,
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

			s.LogViewerUIConfig(w, r)

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

func TestReplaceLogViewerUIConfig(t *testing.T) {
	type fields struct {
		ConfigStore chronograf.ConfigStore
	}
	type args struct {
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
										{
											Type:  "visibility",
											Value: "visible",
										},
										{
											Type:  "label",
											Value: "icon",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
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
								{
									Type:  "visibility",
									Value: "visible",
								},
								{
									Type:  "label",
									Value: "icon",
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
								{
									Type:  "visibility",
									Value: "visible",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/config/logViewer"},"columns":[{"name":"severity","position":1,"encoding":[{"type":"color","value":"info","name":"pineapple"},{"type":"color","value":"emergency","name":"ruby"},{"type":"visibility","value":"visible"},{"type":"label","value":"icon"}]},{"name":"messages","position":0,"encoding":[{"type":"displayName","value":"Log Messages"},{"type":"visibility","value":"visible"}]}]}`,
			},
		},
		{
			name: "Set invalid log viewer configuration – empty",
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
										{
											Type:  "label",
											Value: "icon",
										},
										{
											Type:  "visibility",
											Value: "visible",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				payload: chronograf.LogViewerUIConfig{
					Columns: []chronograf.LogViewerUIColumn{},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"Invalid log viewer UI config: must have at least 1 column"}`,
			},
		},
		{
			name: "Set invalid log viewer configuration - duplicate column name",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						LogViewerUI: chronograf.LogViewerUIConfig{
							Columns: []chronograf.LogViewerUIColumn{
								{
									Name:     "procid",
									Position: 0,
									Encoding: []chronograf.ColumnEncoding{
										{
											Type:  "visibility",
											Value: "hidden",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				payload: chronograf.LogViewerUIConfig{
					Columns: []chronograf.LogViewerUIColumn{
						{
							Name:     "procid",
							Position: 0,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "visibility",
									Value: "hidden",
								},
							},
						},
						{
							Name:     "procid",
							Position: 1,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "visibility",
									Value: "hidden",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"Invalid log viewer UI config: Duplicate column name procid"}`,
			},
		},
		{
			name: "Set invalid log viewer configuration - multiple columns with same position value",
			fields: fields{
				ConfigStore: &mocks.ConfigStore{
					Config: &chronograf.Config{
						LogViewerUI: chronograf.LogViewerUIConfig{
							Columns: []chronograf.LogViewerUIColumn{
								{
									Name:     "procid",
									Position: 0,
									Encoding: []chronograf.ColumnEncoding{
										{
											Type:  "visibility",
											Value: "hidden",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				payload: chronograf.LogViewerUIConfig{
					Columns: []chronograf.LogViewerUIColumn{
						{
							Name:     "procid",
							Position: 0,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "visibility",
									Value: "hidden",
								},
							},
						},
						{
							Name:     "timestamp",
							Position: 0,
							Encoding: []chronograf.ColumnEncoding{
								{
									Type:  "visibility",
									Value: "hidden",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"Invalid log viewer UI config: Multiple columns with same position value"}`,
			},
		},
		{
			name: "Set invalid log viewer configuration – no visibility",
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
										{
											Type:  "label",
											Value: "icon",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
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
								{
									Type:  "label",
									Value: "icon",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json",
				body:        `{"code":400,"message":"Invalid log viewer UI config: missing visibility encoding in column severity"}`,
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
			buf, _ := json.Marshal(tt.args.payload)
			r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.ReplaceLogViewerUIConfig(w, r)

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
