package integrations

// This was intentionally added under the integrations package and not the integrations test package
// so that changes in other parts of the code base that may have an effect on these test will not
// compile until they are fixed.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"net/http"
	"testing"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/roles"
	"github.com/influxdata/chronograf/server"
)

func TestServer(t *testing.T) {
	type fields struct {
		Organizations []chronograf.Organization
		Users         []chronograf.User
		Sources       []chronograf.Source
		Servers       []chronograf.Server
		Layouts       []chronograf.Layout
		Dashboards    []chronograf.Dashboard
		Config        *chronograf.Config
	}
	type args struct {
		server    *server.Server
		method    string
		path      string
		payload   interface{} // Expects this to be a json serializable struct
		principal oauth2.Principal
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name    string
		subName string
		fields  fields
		args    args
		wants   wants
	}{
		{
			name:    "GET /sources/5000",
			subName: "Get specific source; including Canned source",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
							{
								Name:         "viewer",
								Organization: "howdy", // from canned testdata
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/sources/5000",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "id": "5000",
  "name": "Influx 1",
  "type": "influx-enterprise",
  "username": "user1",
  "url": "http://localhost:8086",
  "metaUrl": "http://metaurl.com",
  "default": true,
  "telegraf": "telegraf",
  "organization": "howdy",
  "links": {
    "self": "/chronograf/v1/sources/5000",
    "kapacitors": "/chronograf/v1/sources/5000/kapacitors",
    "proxy": "/chronograf/v1/sources/5000/proxy",
    "queries": "/chronograf/v1/sources/5000/queries",
    "write": "/chronograf/v1/sources/5000/write",
    "permissions": "/chronograf/v1/sources/5000/permissions",
    "users": "/chronograf/v1/sources/5000/users",
    "roles": "/chronograf/v1/sources/5000/roles",
    "databases": "/chronograf/v1/sources/5000/dbs"
  }
}
`,
			},
		},
		{
			name:    "GET /sources/5000/kapacitors/5000",
			subName: "Get specific kapacitors; including Canned kapacitors",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
							{
								Name:         "viewer",
								Organization: "howdy", // from canned testdata
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/sources/5000/kapacitors/5000",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "id": "5000",
  "name": "Kapa 1",
  "url": "http://localhost:9092",
  "active": true,
  "links": {
    "proxy": "/chronograf/v1/sources/5000/kapacitors/5000/proxy",
    "self": "/chronograf/v1/sources/5000/kapacitors/5000",
    "rules": "/chronograf/v1/sources/5000/kapacitors/5000/rules",
    "tasks": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/tasks",
    "ping": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/ping"
  }
}
`,
			},
		},
		{
			name:    "GET /sources/5000/kapacitors",
			subName: "Get all kapacitors; including Canned kapacitors",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
							{
								Name:         "viewer",
								Organization: "howdy", // from canned testdata
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/sources/5000/kapacitors",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "kapacitors": [
    {
      "id": "5000",
      "name": "Kapa 1",
      "url": "http://localhost:9092",
      "active": true,
      "links": {
        "proxy": "/chronograf/v1/sources/5000/kapacitors/5000/proxy",
        "self": "/chronograf/v1/sources/5000/kapacitors/5000",
        "rules": "/chronograf/v1/sources/5000/kapacitors/5000/rules",
        "tasks": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/tasks",
        "ping": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/ping"
      }
    }
  ]
}
`,
			},
		},
		{
			name:    "GET /sources",
			subName: "Get all sources; including Canned sources",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
							{
								Name:         "viewer",
								Organization: "howdy", // from canned testdata
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/sources",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "sources": [
    {
      "id": "5000",
      "name": "Influx 1",
      "type": "influx-enterprise",
      "username": "user1",
      "url": "http://localhost:8086",
      "metaUrl": "http://metaurl.com",
      "default": true,
      "telegraf": "telegraf",
      "organization": "howdy",
      "links": {
        "self": "/chronograf/v1/sources/5000",
        "kapacitors": "/chronograf/v1/sources/5000/kapacitors",
        "proxy": "/chronograf/v1/sources/5000/proxy",
        "queries": "/chronograf/v1/sources/5000/queries",
        "write": "/chronograf/v1/sources/5000/write",
        "permissions": "/chronograf/v1/sources/5000/permissions",
        "users": "/chronograf/v1/sources/5000/users",
        "roles": "/chronograf/v1/sources/5000/roles",
        "databases": "/chronograf/v1/sources/5000/dbs"
      }
    }
  ]
}
`,
			},
		},
		{
			name:    "GET /organizations",
			subName: "Get all organizations; including Canned organization",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/organizations",
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "links": {
    "self": "/chronograf/v1/organizations"
  },
  "organizations": [
    {
      "links": {
        "self": "/chronograf/v1/organizations/default"
      },
      "id": "default",
      "name": "Default",
      "defaultRole": "member",
      "public": true
    },
    {
      "links": {
        "self": "/chronograf/v1/organizations/howdy"
      },
      "id": "howdy",
      "name": "An Organization",
      "defaultRole": "viewer",
      "public": false
    }
  ]
}`,
			},
		},
		{
			name:    "GET /organizations/howdy",
			subName: "Get specific organizations; Canned organization",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/organizations/howdy",
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "links": {
    "self": "/chronograf/v1/organizations/howdy"
  },
  "id": "howdy",
  "name": "An Organization",
  "defaultRole": "viewer",
  "public": false
}`,
			},
		},
		{
			name:    "GET /dashboards/1000",
			subName: "Get specific in the howdy organization; Using Canned testdata",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "howdy",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/dashboards/1000",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "id": 1000,
  "cells": [
    {
      "i": "8f61c619-dd9b-4761-8aa8-577f27247093",
      "x": 0,
      "y": 0,
      "w": 11,
      "h": 5,
      "name": "Untitled Cell",
      "queries": [
        {
          "query": "SELECT mean(\"value\") AS \"mean_value\" FROM \"telegraf\".\"autogen\".\"cpg\" WHERE time > :dashboardTime: GROUP BY :interval: FILL(null)",
          "queryConfig": {
            "database": "telegraf",
            "measurement": "cpg",
            "retentionPolicy": "autogen",
            "fields": [
              {
                "value": "mean",
                "type": "func",
                "alias": "mean_value",
                "args": [
                  {
                    "value": "value",
                    "type": "field",
                    "alias": ""
                  }
                ]
              }
            ],
            "tags": {},
            "groupBy": {
              "time": "auto",
              "tags": []
            },
            "areTagsAccepted": false,
            "fill": "null",
            "rawText": null,
            "range": null,
            "shifts": null
          },
          "source": "/chronograf/v1/sources/2"
        }
      ],
      "axes": {
        "x": {
          "bounds": [],
          "label": "",
          "prefix": "",
          "suffix": "",
          "base": "10",
          "scale": "linear"
        },
        "y": {
          "bounds": [],
          "label": "",
          "prefix": "",
          "suffix": "",
          "base": "10",
          "scale": "linear"
        },
        "y2": {
          "bounds": [],
          "label": "",
          "prefix": "",
          "suffix": "",
          "base": "10",
          "scale": "linear"
        }
      },
      "type": "line",
      "colors": [
        {
          "id": "0",
          "type": "min",
          "hex": "#00C9FF",
          "name": "laser",
          "value": "0"
        },
        {
          "id": "1",
          "type": "max",
          "hex": "#9394FF",
          "name": "comet",
          "value": "100"
        }
      ],
      "legend":{
          "type": "static",
          "orientation": "bottom"
      },
      "links": {
        "self": "/chronograf/v1/dashboards/1000/cells/8f61c619-dd9b-4761-8aa8-577f27247093"
      }
    }
  ],
  "templates": [
    {
      "tempVar": ":dbs:",
      "values": [
        {
          "value": "_internal",
          "type": "database",
          "selected": true
        },
        {
          "value": "telegraf",
          "type": "database",
          "selected": false
        },
        {
          "value": "tensorflowdb",
          "type": "database",
          "selected": false
        },
        {
          "value": "pushgateway",
          "type": "database",
          "selected": false
        },
        {
          "value": "node_exporter",
          "type": "database",
          "selected": false
        },
        {
          "value": "mydb",
          "type": "database",
          "selected": false
        },
        {
          "value": "tiny",
          "type": "database",
          "selected": false
        },
        {
          "value": "blah",
          "type": "database",
          "selected": false
        },
        {
          "value": "test",
          "type": "database",
          "selected": false
        },
        {
          "value": "chronograf",
          "type": "database",
          "selected": false
        },
        {
          "value": "db_name",
          "type": "database",
          "selected": false
        },
        {
          "value": "demo",
          "type": "database",
          "selected": false
        },
        {
          "value": "eeg",
          "type": "database",
          "selected": false
        },
        {
          "value": "solaredge",
          "type": "database",
          "selected": false
        },
        {
          "value": "zipkin",
          "type": "database",
          "selected": false
        }
      ],
      "id": "e7e498bf-5869-4874-9071-24628a2cda63",
      "type": "databases",
      "label": "",
      "query": {
        "influxql": "SHOW DATABASES",
        "measurement": "",
        "tagKey": "",
        "fieldKey": ""
      },
      "links": {
        "self": "/chronograf/v1/dashboards/1000/templates/e7e498bf-5869-4874-9071-24628a2cda63"
      }
    }
  ],
  "name": "Name This Dashboard",
  "organization": "howdy",
  "links": {
    "self": "/chronograf/v1/dashboards/1000",
    "cells": "/chronograf/v1/dashboards/1000/cells",
    "templates": "/chronograf/v1/dashboards/1000/templates"
  }
}`,
			},
		},
		{
			name:    "GET /dashboards",
			subName: "Get all dashboards in the howdy organization; Using Canned testdata",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
							{
								Name:         "admin",
								Organization: "howdy",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/dashboards",
				principal: oauth2.Principal{
					Organization: "howdy",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "dashboards": [
    {
      "id": 1000,
      "cells": [
        {
          "i": "8f61c619-dd9b-4761-8aa8-577f27247093",
          "x": 0,
          "y": 0,
          "w": 11,
          "h": 5,
          "name": "Untitled Cell",
          "queries": [
            {
              "query": "SELECT mean(\"value\") AS \"mean_value\" FROM \"telegraf\".\"autogen\".\"cpg\" WHERE time > :dashboardTime: GROUP BY :interval: FILL(null)",
              "queryConfig": {
                "database": "telegraf",
                "measurement": "cpg",
                "retentionPolicy": "autogen",
                "fields": [
                  {
                    "value": "mean",
                    "type": "func",
                    "alias": "mean_value",
                    "args": [
                      {
                        "value": "value",
                        "type": "field",
                        "alias": ""
                      }
                    ]
                  }
                ],
                "tags": {},
                "groupBy": {
                  "time": "auto",
                  "tags": []
                },
                "areTagsAccepted": false,
                "fill": "null",
                "rawText": null,
                "range": null,
                "shifts": null
              },
              "source": "/chronograf/v1/sources/2"
            }
          ],
          "axes": {
            "x": {
              "bounds": [],
              "label": "",
              "prefix": "",
              "suffix": "",
              "base": "10",
              "scale": "linear"
            },
            "y": {
              "bounds": [],
              "label": "",
              "prefix": "",
              "suffix": "",
              "base": "10",
              "scale": "linear"
            },
            "y2": {
              "bounds": [],
              "label": "",
              "prefix": "",
              "suffix": "",
              "base": "10",
              "scale": "linear"
            }
          },
          "type": "line",
          "colors": [
            {
              "id": "0",
              "type": "min",
              "hex": "#00C9FF",
              "name": "laser",
              "value": "0"
            },
            {
              "id": "1",
              "type": "max",
              "hex": "#9394FF",
              "name": "comet",
              "value": "100"
            }
          ],
          "legend":{
              "type": "static",
              "orientation": "bottom"
          },
          "links": {
            "self": "/chronograf/v1/dashboards/1000/cells/8f61c619-dd9b-4761-8aa8-577f27247093"
          }
        }
      ],
      "templates": [
        {
          "tempVar": ":dbs:",
          "values": [
            {
              "value": "_internal",
              "type": "database",
              "selected": true
            },
            {
              "value": "telegraf",
              "type": "database",
              "selected": false
            },
            {
              "value": "tensorflowdb",
              "type": "database",
              "selected": false
            },
            {
              "value": "pushgateway",
              "type": "database",
              "selected": false
            },
            {
              "value": "node_exporter",
              "type": "database",
              "selected": false
            },
            {
              "value": "mydb",
              "type": "database",
              "selected": false
            },
            {
              "value": "tiny",
              "type": "database",
              "selected": false
            },
            {
              "value": "blah",
              "type": "database",
              "selected": false
            },
            {
              "value": "test",
              "type": "database",
              "selected": false
            },
            {
              "value": "chronograf",
              "type": "database",
              "selected": false
            },
            {
              "value": "db_name",
              "type": "database",
              "selected": false
            },
            {
              "value": "demo",
              "type": "database",
              "selected": false
            },
            {
              "value": "eeg",
              "type": "database",
              "selected": false
            },
            {
              "value": "solaredge",
              "type": "database",
              "selected": false
            },
            {
              "value": "zipkin",
              "type": "database",
              "selected": false
            }
          ],
          "id": "e7e498bf-5869-4874-9071-24628a2cda63",
          "type": "databases",
          "label": "",
          "query": {
            "influxql": "SHOW DATABASES",
            "measurement": "",
            "tagKey": "",
            "fieldKey": ""
          },
          "links": {
            "self": "/chronograf/v1/dashboards/1000/templates/e7e498bf-5869-4874-9071-24628a2cda63"
          }
        }
      ],
      "name": "Name This Dashboard",
      "organization": "howdy",
      "links": {
        "self": "/chronograf/v1/dashboards/1000",
        "cells": "/chronograf/v1/dashboards/1000/cells",
        "templates": "/chronograf/v1/dashboards/1000/templates"
      }
    }
  ]
}`,
			},
		},
		{
			name:    "GET /users",
			subName: "User Not Found in the Default Organization",
			fields: fields{
				Users: []chronograf.User{},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/users",
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 403,
				body:       `{"code":403,"message":"User is not authorized"}`,
			},
		},
		{
			name:    "GET /users",
			subName: "Single User in the Default Organization as SuperAdmin",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "GET",
				path:   "/chronograf/v1/users",
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "links": {
    "self": "/chronograf/v1/users"
  },
  "users": [
    {
      "links": {
        "self": "/chronograf/v1/users/1"
      },
      "id": "1",
      "name": "billibob",
      "provider": "github",
      "scheme": "oauth2",
      "superAdmin": true,
      "roles": [
        {
          "name": "admin",
          "organization": "default"
        }
      ]
    }
  ]
}`,
			},
		},
		{
			name:    "POST /users",
			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is true (the default case); User on Principal is a SuperAdmin",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: true,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "POST",
				path:   "/chronograf/v1/users",
				payload: &chronograf.User{
					Name:     "user",
					Provider: "provider",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name:         roles.EditorRoleName,
							Organization: "default",
						},
					},
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 201,
				body: `
{
  "links": {
    "self": "/chronograf/v1/users/2"
  },
  "id": "2",
  "name": "user",
  "provider": "provider",
  "scheme": "oauth2",
  "superAdmin": true,
  "roles": [
    {
      "name": "editor",
      "organization": "default"
    }
  ]
}`,
			},
		},
		{
			name:    "POST /users",
			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is false; User on Principal is a SuperAdmin",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: false,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "POST",
				path:   "/chronograf/v1/users",
				payload: &chronograf.User{
					Name:     "user",
					Provider: "provider",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name:         roles.EditorRoleName,
							Organization: "default",
						},
					},
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 201,
				body: `
{
  "links": {
    "self": "/chronograf/v1/users/2"
  },
  "id": "2",
  "name": "user",
  "provider": "provider",
  "scheme": "oauth2",
  "superAdmin": false,
  "roles": [
    {
      "name": "editor",
      "organization": "default"
    }
  ]
}`,
			},
		},
		{
			name:    "POST /users",
			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is false; User on Principal is Admin, but not a SuperAdmin",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: false,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: false,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "POST",
				path:   "/chronograf/v1/users",
				payload: &chronograf.User{
					Name:     "user",
					Provider: "provider",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name:         roles.EditorRoleName,
							Organization: "default",
						},
					},
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 201,
				body: `
{
  "links": {
    "self": "/chronograf/v1/users/2"
  },
  "id": "2",
  "name": "user",
  "provider": "provider",
  "scheme": "oauth2",
  "superAdmin": false,
  "roles": [
    {
      "name": "editor",
      "organization": "default"
    }
  ]
}`,
			},
		},
		{
			name:    "POST /users",
			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is true; User on Principal is Admin, but not a SuperAdmin",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: true,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: false,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "POST",
				path:   "/chronograf/v1/users",
				payload: &chronograf.User{
					Name:       "user",
					Provider:   "provider",
					Scheme:     "oauth2",
					SuperAdmin: true,
					Roles: []chronograf.Role{
						{
							Name:         roles.EditorRoleName,
							Organization: "default",
						},
					},
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 401,
				body: `
{
  "code": 401,
  "message": "User does not have authorization required to set SuperAdmin status. See https://github.com/influxdata/chronograf/issues/2601 for more information."
}`,
			},
		},
		{
			name:    "PATCH /users/1",
			subName: "SuperAdmin modifying their own status",
			fields: fields{
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "PATCH",
				path:   "/chronograf/v1/users/1",
				payload: map[string]interface{}{
					"id":         "1",
					"superAdmin": false,
					"roles": []interface{}{
						map[string]interface{}{
							"name":         "admin",
							"organization": "default",
						},
					},
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: http.StatusUnauthorized,
				body: `
{
  "code": 401,
  "message": "user cannot modify their own SuperAdmin status"
}
`,
			},
		},
		{
			name:    "PUT /me",
			subName: "Change SuperAdmins current organization to org they dont belong to",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: false,
					},
				},
				Organizations: []chronograf.Organization{
					{
						ID:          "1",
						Name:        "Sweet",
						DefaultRole: roles.ViewerRoleName,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: true,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "PUT",
				path:   "/chronograf/v1/me",
				payload: map[string]string{
					"organization": "1",
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `
{
  "id": "1",
  "name": "billibob",
  "roles": [
    {
      "name": "admin",
      "organization": "default"
    },
    {
      "name": "viewer",
      "organization": "1"
    }
  ],
  "provider": "github",
  "scheme": "oauth2",
  "superAdmin": true,
  "links": {
    "self": "/chronograf/v1/users/1"
  },
  "organizations": [
    {
      "id": "1",
      "name": "Sweet",
      "defaultRole": "viewer",
      "public": false
    },
    {
      "id": "default",
      "name": "Default",
      "defaultRole": "member",
      "public": true
    }
  ],
  "currentOrganization": {
    "id": "1",
    "name": "Sweet",
    "defaultRole": "viewer",
    "public": false
  }
}`,
			},
		},
		{
			name:    "PUT /me",
			subName: "Change Admin current organization to org they dont belong to",
			fields: fields{
				Config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminNewUsers: false,
					},
				},
				Organizations: []chronograf.Organization{
					{
						ID:          "1",
						Name:        "Sweet",
						DefaultRole: roles.ViewerRoleName,
					},
				},
				Users: []chronograf.User{
					{
						ID:         1, // This is artificial, but should be reflective of the users actual ID
						Name:       "billibob",
						Provider:   "github",
						Scheme:     "oauth2",
						SuperAdmin: false,
						Roles: []chronograf.Role{
							{
								Name:         "admin",
								Organization: "default",
							},
						},
					},
				},
			},
			args: args{
				server: &server.Server{
					GithubClientID:     "not empty",
					GithubClientSecret: "not empty",
				},
				method: "PUT",
				path:   "/chronograf/v1/me",
				payload: map[string]string{
					"organization": "1",
				},
				principal: oauth2.Principal{
					Organization: "default",
					Subject:      "billibob",
					Issuer:       "github",
				},
			},
			wants: wants{
				statusCode: 403,
				body: `
				{
  "code": 403,
  "message": "user not found"
}`,
			},
		},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("%s: %s", tt.name, tt.subName)
		t.Run(testName, func(t *testing.T) {
			ctx := context.TODO()
			// Create Test Server
			host, port := hostAndPort()
			tt.args.server.Host = host
			tt.args.server.Port = port

			// Use testdata directory for the canned data
			tt.args.server.CannedPath = "testdata"
			tt.args.server.ResourcesPath = "testdata"

			// This is so that we can use staticly generate jwts
			tt.args.server.TokenSecret = "secret"

			boltFile := newBoltFile()
			tt.args.server.BoltPath = boltFile

			// Prepopulate BoltDB Database for Server
			boltdb := bolt.NewClient()
			boltdb.Path = boltFile

			logger := log.New(log.ParseLevel("debug"))
			build := chronograf.BuildInfo{
				Version: "pre-1.4.0.0",
				Commit:  "",
			}
			_ = boltdb.Open(ctx, logger, build)

			if tt.fields.Config != nil {
				if err := boltdb.ConfigStore.Update(ctx, tt.fields.Config); err != nil {
					t.Fatalf("failed to update global application config %v", err)
					return
				}
			}

			// Populate Organizations
			for i, organization := range tt.fields.Organizations {
				o, err := boltdb.OrganizationsStore.Add(ctx, &organization)
				if err != nil {
					t.Fatalf("failed to add organization: %v", err)
					return
				}
				tt.fields.Organizations[i] = *o
			}

			// Populate Users
			for i, user := range tt.fields.Users {
				u, err := boltdb.UsersStore.Add(ctx, &user)
				if err != nil {
					t.Fatalf("failed to add user: %v", err)
					return
				}
				tt.fields.Users[i] = *u
			}

			// Populate Sources
			for i, source := range tt.fields.Sources {
				s, err := boltdb.SourcesStore.Add(ctx, source)
				if err != nil {
					t.Fatalf("failed to add source: %v", err)
					return
				}
				tt.fields.Sources[i] = s
			}

			// Populate Servers
			for i, server := range tt.fields.Servers {
				s, err := boltdb.ServersStore.Add(ctx, server)
				if err != nil {
					t.Fatalf("failed to add server: %v", err)
					return
				}
				tt.fields.Servers[i] = s
			}

			// Populate Layouts
			for i, layout := range tt.fields.Layouts {
				l, err := boltdb.LayoutsStore.Add(ctx, layout)
				if err != nil {
					t.Fatalf("failed to add layout: %v", err)
					return
				}
				tt.fields.Layouts[i] = l
			}

			// Populate Dashboards
			for i, dashboard := range tt.fields.Dashboards {
				d, err := boltdb.DashboardsStore.Add(ctx, dashboard)
				if err != nil {
					t.Fatalf("failed to add dashboard: %v", err)
					return
				}
				tt.fields.Dashboards[i] = d
			}

			_ = boltdb.Close()

			go tt.args.server.Serve(ctx)
			serverURL := fmt.Sprintf("http://%v:%v%v", host, port, tt.args.path)

			// Wait for the server to come online
			timeout := time.Now().Add(5 * time.Second)
			for {
				_, err := http.Get(serverURL + "/swagger.json")
				if err == nil {
					break
				}
				if time.Now().After(timeout) {
					t.Fatalf("failed to start server")
					return
				}
			}

			// Set the Expiry time on the principal
			tt.args.principal.IssuedAt = time.Now()
			tt.args.principal.ExpiresAt = time.Now().Add(10 * time.Second)

			// Construct HTTP Request
			buf, _ := json.Marshal(tt.args.payload)
			reqBody := ioutil.NopCloser(bytes.NewReader(buf))
			req, _ := http.NewRequest(tt.args.method, serverURL, reqBody)
			token, _ := oauth2.NewJWT(tt.args.server.TokenSecret).Create(ctx, tt.args.principal)
			req.AddCookie(&http.Cookie{
				Name:     "session",
				Value:    string(token),
				HttpOnly: true,
				Path:     "/",
			})

			// Make actual http request
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to make httprequest: %v", err)
				return
			}

			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wants.statusCode {
				t.Errorf(
					"%s %s Status Code = %v, want %v",
					tt.args.method,
					tt.args.path,
					resp.StatusCode,
					tt.wants.statusCode,
				)
			}

			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf(
					"%s %s Content Type = %v, want %v",
					tt.args.method,
					tt.args.path,
					content,
					tt.wants.contentType,
				)
			}

			if eq, err := jsonEqual(tt.wants.body, string(body)); err != nil || !eq {
				t.Errorf(
					"%s %s Body = %v, want %v",
					tt.args.method,
					tt.args.path,
					string(body),
					tt.wants.body,
				)
			}

			tt.args.server.Listener.Close()
		})
	}
}
