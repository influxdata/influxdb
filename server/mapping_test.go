package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/roles"
)

func TestMappings_All(t *testing.T) {
	type fields struct {
		MappingsStore chronograf.MappingsStore
	}
	type args struct {
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
			name: "get all mappings",
			fields: fields{
				MappingsStore: &mocks.MappingsStore{
					AllF: func(ctx context.Context) ([]chronograf.Mapping, error) {
						return []chronograf.Mapping{
							{
								Organization:         "0",
								Provider:             chronograf.MappingWildcard,
								Scheme:               chronograf.MappingWildcard,
								ProviderOrganization: chronograf.MappingWildcard,
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/mappings"},"mappings":[{"links":{"self":"/chronograf/v1/mappings/"},"id":"","organizationId":"0","provider":"*","scheme":"*","providerOrganization":"*"}]}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					MappingsStore: tt.fields.MappingsStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)
			s.Mappings(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Mappings() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Mappings() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Mappings() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestMappings_Add(t *testing.T) {
	type fields struct {
		MappingsStore      chronograf.MappingsStore
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		mapping *chronograf.Mapping
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
			name: "create new mapping",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "0",
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
						}, nil
					},
				},
				MappingsStore: &mocks.MappingsStore{
					AddF: func(ctx context.Context, m *chronograf.Mapping) (*chronograf.Mapping, error) {
						m.ID = "0"
						return m, nil
					},
				},
			},
			args: args{
				mapping: &chronograf.Mapping{
					Organization:         "0",
					Provider:             "*",
					Scheme:               "*",
					ProviderOrganization: "*",
				},
			},
			wants: wants{
				statusCode:  201,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/mappings/0"},"id":"0","organizationId":"0","provider":"*","scheme":"*","providerOrganization":"*"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					MappingsStore:      tt.fields.MappingsStore,
					OrganizationsStore: tt.fields.OrganizationsStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)

			buf, _ := json.Marshal(tt.args.mapping)
			r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.NewMapping(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Add() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Add() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Add() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestMappings_Update(t *testing.T) {
	type fields struct {
		MappingsStore      chronograf.MappingsStore
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		mapping *chronograf.Mapping
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
			name: "update new mapping",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "0",
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
						}, nil
					},
				},
				MappingsStore: &mocks.MappingsStore{
					UpdateF: func(ctx context.Context, m *chronograf.Mapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: &chronograf.Mapping{
					ID:                   "1",
					Organization:         "0",
					Provider:             "*",
					Scheme:               "*",
					ProviderOrganization: "*",
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json",
				body:        `{"links":{"self":"/chronograf/v1/mappings/1"},"id":"1","organizationId":"0","provider":"*","scheme":"*","providerOrganization":"*"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					MappingsStore:      tt.fields.MappingsStore,
					OrganizationsStore: tt.fields.OrganizationsStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)

			buf, _ := json.Marshal(tt.args.mapping)
			r.Body = ioutil.NopCloser(bytes.NewReader(buf))
			r = r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.mapping.ID,
					},
				}))

			s.UpdateMapping(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Add() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Add() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Add() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestMappings_Remove(t *testing.T) {
	type fields struct {
		MappingsStore chronograf.MappingsStore
	}
	type args struct {
		id string
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
			name: "remove mapping",
			fields: fields{
				MappingsStore: &mocks.MappingsStore{
					GetF: func(ctx context.Context, id string) (*chronograf.Mapping, error) {
						return &chronograf.Mapping{
							ID:                   "1",
							Organization:         "0",
							Provider:             "*",
							Scheme:               "*",
							ProviderOrganization: "*",
						}, nil
					},
					DeleteF: func(ctx context.Context, m *chronograf.Mapping) error {
						return nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode: 204,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					MappingsStore: tt.fields.MappingsStore,
				},
				Logger: log.New(log.DebugLevel),
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			s.RemoveMapping(w, r)

			resp := w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. Remove() = %v, want %v", tt.name, resp.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. Remove() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. Remove() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
