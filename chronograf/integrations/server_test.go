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

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/bolt"
	"github.com/influxdata/platform/chronograf/log"
	"github.com/influxdata/platform/chronograf/oauth2"
	"github.com/influxdata/platform/chronograf/server"
)

func TestServer(t *testing.T) {
	type fields struct {
		Organizations []chronograf.Organization
		Mappings      []chronograf.Mapping
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
	//		{
	//			name:    "GET /sources/5000",
	//			subName: "Get specific source; including Canned source",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "viewer",
	//								Organization: "howdy", // from canned testdata
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/sources/5000",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "id": "5000",
	//  "name": "Influx 1",
	//  "type": "influx-enterprise",
	//  "username": "user1",
	//  "url": "http://localhost:8086",
	//  "metaUrl": "http://metaurl.com",
	//  "default": true,
	//  "telegraf": "telegraf",
	//  "organization": "howdy",
	//	"defaultRP": "",
	//	"authentication": "basic",
	//  "links": {
	//    "self": "/chronograf/v1/sources/5000",
	//    "kapacitors": "/chronograf/v1/sources/5000/kapacitors",
	//    "services": "/chronograf/v1/sources/5000/services",
	//    "proxy": "/chronograf/v1/sources/5000/proxy",
	//    "queries": "/chronograf/v1/sources/5000/queries",
	//    "write": "/chronograf/v1/sources/5000/write",
	//    "permissions": "/chronograf/v1/sources/5000/permissions",
	//    "users": "/chronograf/v1/sources/5000/users",
	//    "roles": "/chronograf/v1/sources/5000/roles",
	//    "databases": "/chronograf/v1/sources/5000/dbs",
	//    "annotations": "/chronograf/v1/sources/5000/annotations",
	//    "health": "/chronograf/v1/sources/5000/health"
	//  }
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /sources/5000/kapacitors/5000",
	//			subName: "Get specific kapacitors; including Canned kapacitors",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "viewer",
	//								Organization: "howdy", // from canned testdata
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/sources/5000/kapacitors/5000",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "id": "5000",
	//  "name": "Kapa 1",
	//  "url": "http://localhost:9092",
	//  "active": true,
	//  "insecureSkipVerify": false,
	//  "links": {
	//    "proxy": "/chronograf/v1/sources/5000/kapacitors/5000/proxy",
	//    "self": "/chronograf/v1/sources/5000/kapacitors/5000",
	//    "rules": "/chronograf/v1/sources/5000/kapacitors/5000/rules",
	//    "tasks": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/tasks",
	//    "ping": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/ping"
	//  }
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /sources/5000/kapacitors",
	//			subName: "Get all kapacitors; including Canned kapacitors",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "viewer",
	//								Organization: "howdy", // from canned testdata
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/sources/5000/kapacitors",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "kapacitors": [
	//    {
	//      "id": "5000",
	//      "name": "Kapa 1",
	//      "url": "http://localhost:9092",
	//      "active": true,
	//      "insecureSkipVerify": false,
	//      "links": {
	//        "proxy": "/chronograf/v1/sources/5000/kapacitors/5000/proxy",
	//        "self": "/chronograf/v1/sources/5000/kapacitors/5000",
	//        "rules": "/chronograf/v1/sources/5000/kapacitors/5000/rules",
	//        "tasks": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/tasks",
	//        "ping": "/chronograf/v1/sources/5000/kapacitors/5000/proxy?path=/kapacitor/v1/ping"
	//      }
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /sources",
	//			subName: "Get all sources; including Canned sources",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "viewer",
	//								Organization: "howdy", // from canned testdata
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/sources",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "sources": [
	//    {
	//      "id": "5000",
	//      "name": "Influx 1",
	//      "type": "influx-enterprise",
	//      "username": "user1",
	//      "url": "http://localhost:8086",
	//      "metaUrl": "http://metaurl.com",
	//      "default": true,
	//      "telegraf": "telegraf",
	//      "organization": "howdy",
	//			"defaultRP": "",
	//			"authentication": "basic",
	//      "links": {
	//        "self": "/chronograf/v1/sources/5000",
	//        "kapacitors": "/chronograf/v1/sources/5000/kapacitors",
	//        "services": "/chronograf/v1/sources/5000/services",
	//        "proxy": "/chronograf/v1/sources/5000/proxy",
	//        "queries": "/chronograf/v1/sources/5000/queries",
	//        "write": "/chronograf/v1/sources/5000/write",
	//        "permissions": "/chronograf/v1/sources/5000/permissions",
	//        "users": "/chronograf/v1/sources/5000/users",
	//        "roles": "/chronograf/v1/sources/5000/roles",
	//        "databases": "/chronograf/v1/sources/5000/dbs",
	//        "annotations": "/chronograf/v1/sources/5000/annotations",
	//        "health": "/chronograf/v1/sources/5000/health"
	//      }
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /organizations",
	//			subName: "Get all organizations; including Canned organization",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations"
	//  },
	//  "organizations": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/organizations/default"
	//      },
	//      "id": "default",
	//      "name": "Default",
	//      "defaultRole": "member"
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/organizations/howdy"
	//      },
	//      "id": "howdy",
	//      "name": "An Organization",
	//      "defaultRole": "viewer"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /organizations/howdy",
	//			subName: "Get specific organizations; Canned organization",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations/howdy",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/howdy"
	//  },
	//  "id": "howdy",
	//  "name": "An Organization",
	//  "defaultRole": "viewer"
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /dashboards/1000",
	//			subName: "Get specific in the howdy organization; Using Canned testdata",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "howdy",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/dashboards/1000",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "id": 1000,
	//  "cells": [
	//    {
	//      "i": "8f61c619-dd9b-4761-8aa8-577f27247093",
	//      "x": 0,
	//      "y": 0,
	//      "w": 11,
	//      "h": 5,
	//      "name": "Untitled Cell",
	//      "queries": [
	//        {
	//          "query": "SELECT mean(\"value\") AS \"mean_value\" FROM \"telegraf\".\"autogen\".\"cpg\" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)",
	//          "queryConfig": {
	//            "database": "telegraf",
	//            "measurement": "cpg",
	//            "retentionPolicy": "autogen",
	//            "fields": [
	//              {
	//                "value": "mean",
	//                "type": "func",
	//                "alias": "mean_value",
	//                "args": [
	//                  {
	//                    "value": "value",
	//                    "type": "field",
	//                    "alias": ""
	//                  }
	//                ]
	//              }
	//            ],
	//            "tags": {},
	//            "groupBy": {
	//              "time": "auto",
	//              "tags": []
	//            },
	//            "areTagsAccepted": false,
	//            "fill": "null",
	//            "rawText": null,
	//            "range": null,
	//            "shifts": null
	//          },
	//          "source": "/chronograf/v1/sources/2"
	//        }
	//      ],
	//      "axes": {
	//        "x": {
	//          "bounds": [],
	//          "label": "",
	//          "prefix": "",
	//          "suffix": "",
	//          "base": "10",
	//          "scale": "linear"
	//        },
	//        "y": {
	//          "bounds": [],
	//          "label": "",
	//          "prefix": "",
	//          "suffix": "",
	//          "base": "10",
	//          "scale": "linear"
	//        },
	//        "y2": {
	//          "bounds": [],
	//          "label": "",
	//          "prefix": "",
	//          "suffix": "",
	//          "base": "10",
	//          "scale": "linear"
	//        }
	//      },
	//      "type": "line",
	//      "colors": [
	//        {
	//          "id": "0",
	//          "type": "min",
	//          "hex": "#00C9FF",
	//          "name": "laser",
	//          "value": "0"
	//        },
	//        {
	//          "id": "1",
	//          "type": "max",
	//          "hex": "#9394FF",
	//          "name": "comet",
	//          "value": "100"
	//        }
	//      ],
	//      "legend":{
	//          "type": "static",
	//          "orientation": "bottom"
	//      },
	//      "tableOptions":{
	//        "verticalTimeAxis": false,
	//        "sortBy":{
	//          "internalName": "",
	//          "displayName": "",
	//          "visible": false
	//        },
	//        "wrapping": "",
	//        "fixFirstColumn": false
	//      },
	//      "fieldOptions": null,
	//      "timeFormat": "",
	//      "decimalPlaces":{
	//        "isEnforced": false,
	//        "digits": 0
	//      },
	//      "links": {
	//        "self": "/chronograf/v1/dashboards/1000/cells/8f61c619-dd9b-4761-8aa8-577f27247093"
	//      }
	//    }
	//  ],
	//  "templates": [
	//    {
	//      "tempVar": ":dbs:",
	//      "values": [
	//        {
	//          "value": "_internal",
	//          "type": "database",
	//          "selected": true
	//        },
	//        {
	//          "value": "telegraf",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "tensorflowdb",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "pushgateway",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "node_exporter",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "mydb",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "tiny",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "blah",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "test",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "chronograf",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "db_name",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "demo",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "eeg",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "solaredge",
	//          "type": "database",
	//          "selected": false
	//        },
	//        {
	//          "value": "zipkin",
	//          "type": "database",
	//          "selected": false
	//        }
	//      ],
	//      "id": "e7e498bf-5869-4874-9071-24628a2cda63",
	//      "type": "databases",
	//      "label": "",
	//      "query": {
	//        "influxql": "SHOW DATABASES",
	//        "measurement": "",
	//        "tagKey": "",
	//        "fieldKey": ""
	//      },
	//      "links": {
	//        "self": "/chronograf/v1/dashboards/1000/templates/e7e498bf-5869-4874-9071-24628a2cda63"
	//      }
	//    }
	//  ],
	//  "name": "Name This Dashboard",
	//  "organization": "howdy",
	//  "links": {
	//    "self": "/chronograf/v1/dashboards/1000",
	//    "cells": "/chronograf/v1/dashboards/1000/cells",
	//    "templates": "/chronograf/v1/dashboards/1000/templates"
	//  }
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /dashboards",
	//			subName: "Get all dashboards in the howdy organization; Using Canned testdata",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "admin",
	//								Organization: "howdy",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/dashboards",
	//				principal: oauth2.Principal{
	//					Organization: "howdy",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "dashboards": [
	//    {
	//      "id": 1000,
	//      "cells": [
	//        {
	//          "i": "8f61c619-dd9b-4761-8aa8-577f27247093",
	//          "x": 0,
	//          "y": 0,
	//          "w": 11,
	//          "h": 5,
	//          "name": "Untitled Cell",
	//          "queries": [
	//            {
	//              "query": "SELECT mean(\"value\") AS \"mean_value\" FROM \"telegraf\".\"autogen\".\"cpg\" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)",
	//              "queryConfig": {
	//                "database": "telegraf",
	//                "measurement": "cpg",
	//                "retentionPolicy": "autogen",
	//                "fields": [
	//                  {
	//                    "value": "mean",
	//                    "type": "func",
	//                    "alias": "mean_value",
	//                    "args": [
	//                      {
	//                        "value": "value",
	//                        "type": "field",
	//                        "alias": ""
	//                      }
	//                    ]
	//                  }
	//                ],
	//                "tags": {},
	//                "groupBy": {
	//                  "time": "auto",
	//                  "tags": []
	//                },
	//                "areTagsAccepted": false,
	//                "fill": "null",
	//                "rawText": null,
	//                "range": null,
	//                "shifts": null
	//              },
	//              "source": "/chronograf/v1/sources/2"
	//            }
	//          ],
	//          "axes": {
	//            "x": {
	//              "bounds": [],
	//              "label": "",
	//              "prefix": "",
	//              "suffix": "",
	//              "base": "10",
	//              "scale": "linear"
	//            },
	//            "y": {
	//              "bounds": [],
	//              "label": "",
	//              "prefix": "",
	//              "suffix": "",
	//              "base": "10",
	//              "scale": "linear"
	//            },
	//            "y2": {
	//              "bounds": [],
	//              "label": "",
	//              "prefix": "",
	//              "suffix": "",
	//              "base": "10",
	//              "scale": "linear"
	//            }
	//          },
	//          "type": "line",
	//          "colors": [
	//            {
	//              "id": "0",
	//              "type": "min",
	//              "hex": "#00C9FF",
	//              "name": "laser",
	//              "value": "0"
	//            },
	//            {
	//              "id": "1",
	//              "type": "max",
	//              "hex": "#9394FF",
	//              "name": "comet",
	//              "value": "100"
	//            }
	//           ],
	//          "legend": {
	//            "type": "static",
	//            "orientation": "bottom"
	//          },
	//          "tableOptions":{
	//            "verticalTimeAxis": false,
	//            "sortBy":{
	//              "internalName": "",
	//              "displayName": "",
	//              "visible": false
	//            },
	//            "wrapping": "",
	//            "fixFirstColumn": false
	//          },
	//          "fieldOptions": null,
	//          "timeFormat": "",
	//          "decimalPlaces":{
	//            "isEnforced": false,
	//            "digits": 0
	//          },
	//          "links": {
	//            "self": "/chronograf/v1/dashboards/1000/cells/8f61c619-dd9b-4761-8aa8-577f27247093"
	//          }
	//        }
	//      ],
	//      "templates": [
	//        {
	//          "tempVar": ":dbs:",
	//          "values": [
	//            {
	//              "value": "_internal",
	//              "type": "database",
	//              "selected": true
	//            },
	//            {
	//              "value": "telegraf",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "tensorflowdb",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "pushgateway",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "node_exporter",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "mydb",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "tiny",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "blah",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "test",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "chronograf",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "db_name",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "demo",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "eeg",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "solaredge",
	//              "type": "database",
	//              "selected": false
	//            },
	//            {
	//              "value": "zipkin",
	//              "type": "database",
	//              "selected": false
	//            }
	//          ],
	//          "id": "e7e498bf-5869-4874-9071-24628a2cda63",
	//          "type": "databases",
	//          "label": "",
	//          "query": {
	//            "influxql": "SHOW DATABASES",
	//            "measurement": "",
	//            "tagKey": "",
	//            "fieldKey": ""
	//          },
	//          "links": {
	//            "self": "/chronograf/v1/dashboards/1000/templates/e7e498bf-5869-4874-9071-24628a2cda63"
	//          }
	//        }
	//      ],
	//      "name": "Name This Dashboard",
	//      "organization": "howdy",
	//      "links": {
	//        "self": "/chronograf/v1/dashboards/1000",
	//        "cells": "/chronograf/v1/dashboards/1000/cells",
	//        "templates": "/chronograf/v1/dashboards/1000/templates"
	//      }
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /users",
	//			subName: "User Not Found in the Default Organization",
	//			fields: fields{
	//				Users: []chronograf.User{},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 403,
	//				body:       `{"code":403,"message":"User is not authorized"}`,
	//			},
	//		},
	//		{
	//			name:    "GET /users",
	//			subName: "Single User in the Default Organization as SuperAdmin",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users"
	//  },
	//  "users": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/organizations/default/users/1"
	//      },
	//      "id": "1",
	//      "name": "billibob",
	//      "provider": "github",
	//      "scheme": "oauth2",
	//      "superAdmin": true,
	//      "roles": [
	//        {
	//          "name": "admin",
	//          "organization": "default"
	//        }
	//      ]
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /users",
	//			subName: "Two users in two organizations; user making request is as SuperAdmin with out raw query param",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//					{
	//						ID:         2, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billietta",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "cool",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users"
	//  },
	//  "users": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/organizations/default/users/1"
	//      },
	//      "id": "1",
	//      "name": "billibob",
	//      "provider": "github",
	//      "scheme": "oauth2",
	//      "superAdmin": true,
	//      "roles": [
	//        {
	//          "name": "admin",
	//          "organization": "default"
	//        }
	//      ]
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "User making request is as SuperAdmin with raw query param;  being created has wildcard role",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         "*",
	//							Organization: "default",
	//						},
	//					},
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": false,
	//  "roles": [
	//    {
	//      "name": "member",
	//      "organization": "default"
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "User making request is as SuperAdmin with raw query param;  being created has no roles",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles:    []chronograf.Role{},
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": false,
	//  "roles": []
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /users",
	//			subName: "Two users in two organizations; user making request is as SuperAdmin with raw query param",
	//			fields: fields{
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//					{
	//						ID:         2, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billietta",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "1",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users"
	//  },
	//  "users": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/users/1"
	//      },
	//      "id": "1",
	//      "name": "billibob",
	//      "provider": "github",
	//      "scheme": "oauth2",
	//      "superAdmin": true,
	//      "roles": [
	//        {
	//          "name": "admin",
	//          "organization": "default"
	//        }
	//      ]
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/users/2"
	//      },
	//      "id": "2",
	//      "name": "billietta",
	//      "provider": "github",
	//      "scheme": "oauth2",
	//      "superAdmin": true,
	//      "roles": [
	//        {
	//          "name": "admin",
	//          "organization": "1"
	//        }
	//      ]
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /users",
	//			subName: "Two users in two organizations; user making request is as not SuperAdmin with raw query param",
	//			fields: fields{
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//					{
	//						ID:         2, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billietta",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "admin",
	//								Organization: "1",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/users",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billieta",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 403,
	//				body: `
	//{
	//  "code": 403,
	//  "message": "User is not authorized"
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is true (the default case); User on Principal is a SuperAdmin",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "default",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": true,
	//  "roles": [
	//    {
	//      "name": "editor",
	//      "organization": "default"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is false; User on Principal is a SuperAdmin",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "default",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": false,
	//  "roles": [
	//    {
	//      "name": "editor",
	//      "organization": "default"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is false; User on Principal is Admin, but not a SuperAdmin",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "default",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": false,
	//  "roles": [
	//    {
	//      "name": "editor",
	//      "organization": "default"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "Create a New User with SuperAdmin status; SuperAdminNewUsers is true; User on Principal is Admin, but not a SuperAdmin",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				payload: &chronograf.User{
	//					Name:       "user",
	//					Provider:   "provider",
	//					Scheme:     "oauth2",
	//					SuperAdmin: true,
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "default",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 401,
	//				body: `
	//{
	//  "code": 401,
	//  "message": "User does not have authorization required to set SuperAdmin status. See https://github.com/influxdata/platform/chronograf/issues/2601 for more information."
	//}`,
	//			},
	//		},
	//		{
	//			name:    "POST /users",
	//			subName: "Create a New User with in multiple organizations; User on Principal is a SuperAdmin with raw query param",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/users",
	//				payload: &chronograf.User{
	//					Name:     "user",
	//					Provider: "provider",
	//					Scheme:   "oauth2",
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "default",
	//						},
	//						{
	//							Name:         roles.EditorRoleName,
	//							Organization: "1",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users/2"
	//  },
	//  "id": "2",
	//  "name": "user",
	//  "provider": "provider",
	//  "scheme": "oauth2",
	//  "superAdmin": true,
	//  "roles": [
	//    {
	//      "name": "editor",
	//      "organization": "default"
	//    },
	//    {
	//      "name": "editor",
	//      "organization": "1"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "PATCH /users",
	//			subName: "Update user to have no roles",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PATCH",
	//				path:   "/chronograf/v1/users/1",
	//				payload: map[string]interface{}{
	//					"name":       "billibob",
	//					"provider":   "github",
	//					"scheme":     "oauth2",
	//					"superAdmin": true,
	//					"roles":      []chronograf.Role{},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users/1"
	//  },
	//  "id": "1",
	//  "name": "billibob",
	//  "provider": "github",
	//  "scheme": "oauth2",
	//  "superAdmin": true,
	//  "roles": [
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "PATCH /users",
	//			subName: "Update user roles with wildcard",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PATCH",
	//				path:   "/chronograf/v1/users/1",
	//				payload: &chronograf.User{
	//					Name:       "billibob",
	//					Provider:   "github",
	//					Scheme:     "oauth2",
	//					SuperAdmin: true,
	//					Roles: []chronograf.Role{
	//						{
	//							Name:         roles.AdminRoleName,
	//							Organization: "default",
	//						},
	//						{
	//							Name:         roles.WildcardRoleName,
	//							Organization: "1",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/users/1"
	//  },
	//  "id": "1",
	//  "name": "billibob",
	//  "provider": "github",
	//  "scheme": "oauth2",
	//  "superAdmin": true,
	//  "roles": [
	//    {
	//      "name": "admin",
	//      "organization": "default"
	//    },
	//    {
	//      "name": "viewer",
	//      "organization": "1"
	//    }
	//  ]
	//}`,
	//			},
	//		},
	//		{
	//			name:    "PATCH /users/1",
	//			subName: "SuperAdmin modifying their own status",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PATCH",
	//				path:   "/chronograf/v1/organizations/default/users/1",
	//				payload: map[string]interface{}{
	//					"id":         "1",
	//					"superAdmin": false,
	//					"roles": []interface{}{
	//						map[string]interface{}{
	//							"name":         "admin",
	//							"organization": "default",
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: http.StatusUnauthorized,
	//				body: `
	//{
	//  "code": 401,
	//  "message": "user cannot modify their own SuperAdmin status"
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /organization/default/users",
	//			subName: "Organization not set explicitly on principal",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/organizations/default/users",
	//				principal: oauth2.Principal{
	//					Organization: "",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users"
	//  },
	//  "users": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/organizations/default/users/1"
	//      },
	//      "id": "1",
	//      "name": "billibob",
	//      "provider": "github",
	//      "scheme": "oauth2",
	//      "superAdmin": true,
	//      "roles": [
	//        {
	//          "name": "admin",
	//          "organization": "default"
	//        }
	//      ]
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "PUT /me",
	//			subName: "Change SuperAdmins current organization to org they dont belong to",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PUT",
	//				path:   "/chronograf/v1/me",
	//				payload: map[string]string{
	//					"organization": "1",
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "id": "1",
	//  "name": "billibob",
	//  "roles": [
	//    {
	//      "name": "admin",
	//      "organization": "default"
	//    },
	//    {
	//      "name": "viewer",
	//      "organization": "1"
	//    }
	//  ],
	//  "provider": "github",
	//  "scheme": "oauth2",
	//  "superAdmin": true,
	//  "links": {
	//    "self": "/chronograf/v1/organizations/1/users/1"
	//  },
	//  "organizations": [
	//      {
	//        "id": "1",
	//        "name": "Sweet",
	//        "defaultRole": "viewer"
	//      },
	//      {
	//        "id": "default",
	//        "name": "Default",
	//        "defaultRole": "member"
	//      }
	//  ],
	//  "currentOrganization": {
	//    "id": "1",
	//    "name": "Sweet",
	//    "defaultRole": "viewer"
	//  }
	//}`,
	//			},
	//		},
	//		{
	//			name:    "PUT /me",
	//			subName: "Change Admin current organization to org they dont belong to",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PUT",
	//				path:   "/chronograf/v1/me",
	//				payload: map[string]string{
	//					"organization": "1",
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 403,
	//				body: `
	//        {
	//  "code": 403,
	//  "message": "user not found"
	//}`,
	//			},
	//		},
	//		{
	//			name:    "GET /me",
	//			subName: "New user hits me for the first time",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Mappings: []chronograf.Mapping{
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "influxdata",
	//					},
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "2",
	//						Organization:         "2",
	//						Provider:             "github",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "3",
	//						Organization:         "3",
	//						Provider:             "auth0",
	//						Scheme:               "ldap",
	//						ProviderOrganization: "*",
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//					{
	//						ID:          "2",
	//						Name:        "What",
	//						DefaultRole: roles.EditorRoleName,
	//					},
	//					{
	//						ID:          "3",
	//						Name:        "Okay",
	//						DefaultRole: roles.AdminRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles:      []chronograf.Role{},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/me",
	//				principal: oauth2.Principal{
	//					Subject: "billietta",
	//					Issuer:  "github",
	//					Group:   "influxdata,idk,mimi",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "id": "2",
	//  "name": "billietta",
	//  "roles": [
	//    {
	//      "name": "viewer",
	//      "organization": "1"
	//    },
	//    {
	//      "name": "editor",
	//      "organization": "2"
	//    },
	//    {
	//      "name": "member",
	//      "organization": "default"
	//    }
	//  ],
	//  "provider": "github",
	//  "scheme": "oauth2",
	//  "links": {
	//    "self": "/chronograf/v1/organizations/default/users/2"
	//  },
	//  "organizations": [
	//    {
	//      "id": "1",
	//      "name": "Sweet",
	//      "defaultRole": "viewer"
	//    },
	//    {
	//      "id": "2",
	//      "name": "What",
	//      "defaultRole": "editor"
	//    },
	//    {
	//      "id": "default",
	//      "name": "Default",
	//      "defaultRole": "member"
	//    }
	//  ],
	//  "currentOrganization": {
	//    "id": "default",
	//    "name": "Default",
	//    "defaultRole": "member"
	//  }
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /mappings",
	//			subName: "get all mappings",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Mappings: []chronograf.Mapping{
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "influxdata",
	//					},
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "2",
	//						Organization:         "2",
	//						Provider:             "github",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "3",
	//						Organization:         "3",
	//						Provider:             "auth0",
	//						Scheme:               "ldap",
	//						ProviderOrganization: "*",
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//					{
	//						ID:          "2",
	//						Name:        "What",
	//						DefaultRole: roles.EditorRoleName,
	//					},
	//					{
	//						ID:          "3",
	//						Name:        "Okay",
	//						DefaultRole: roles.AdminRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/mappings",
	//				principal: oauth2.Principal{
	//					Subject: "billibob",
	//					Issuer:  "github",
	//					Group:   "influxdata,idk,mimi",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/mappings"
	//  },
	//  "mappings": [
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/mappings/1"
	//      },
	//      "id": "1",
	//      "organizationId": "1",
	//      "provider": "*",
	//      "scheme": "*",
	//      "providerOrganization": "influxdata"
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/mappings/2"
	//      },
	//      "id": "2",
	//      "organizationId": "1",
	//      "provider": "*",
	//      "scheme": "*",
	//      "providerOrganization": "*"
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/mappings/3"
	//      },
	//      "id": "3",
	//      "organizationId": "2",
	//      "provider": "github",
	//      "scheme": "*",
	//      "providerOrganization": "*"
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/mappings/4"
	//      },
	//      "id": "4",
	//      "organizationId": "3",
	//      "provider": "auth0",
	//      "scheme": "ldap",
	//      "providerOrganization": "*"
	//    },
	//    {
	//      "links": {
	//        "self": "/chronograf/v1/mappings/default"
	//      },
	//      "id": "default",
	//      "organizationId": "default",
	//      "provider": "*",
	//      "scheme": "*",
	//      "providerOrganization": "*"
	//    }
	//  ]
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /mappings",
	//			subName: "get all mappings - user is not super admin",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Mappings: []chronograf.Mapping{
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "influxdata",
	//					},
	//					{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "2",
	//						Organization:         "2",
	//						Provider:             "github",
	//						Scheme:               "*",
	//						ProviderOrganization: "*",
	//					},
	//					{
	//						ID:                   "3",
	//						Organization:         "3",
	//						Provider:             "auth0",
	//						Scheme:               "ldap",
	//						ProviderOrganization: "*",
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//					{
	//						ID:          "2",
	//						Name:        "What",
	//						DefaultRole: roles.EditorRoleName,
	//					},
	//					{
	//						ID:          "3",
	//						Name:        "Okay",
	//						DefaultRole: roles.AdminRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/mappings",
	//				principal: oauth2.Principal{
	//					Subject: "billibob",
	//					Issuer:  "github",
	//					Group:   "influxdata,idk,mimi",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 403,
	//				body: `
	//{
	//  "code": 403,
	//  "message": "User is not authorized"
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "POST /mappings",
	//			subName: "create new mapping",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Mappings: []chronograf.Mapping{},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "POST",
	//				path:   "/chronograf/v1/mappings",
	//				payload: &chronograf.Mapping{
	//					ID:                   "1",
	//					Organization:         "1",
	//					Provider:             "*",
	//					Scheme:               "*",
	//					ProviderOrganization: "influxdata",
	//				},
	//				principal: oauth2.Principal{
	//					Subject: "billibob",
	//					Issuer:  "github",
	//					Group:   "influxdata,idk,mimi",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 201,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/mappings/1"
	//  },
	//  "id": "1",
	//  "organizationId": "1",
	//  "provider": "*",
	//  "scheme": "*",
	//  "providerOrganization": "influxdata"
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "PUT /mappings",
	//			subName: "update new mapping",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: false,
	//					},
	//				},
	//				Mappings: []chronograf.Mapping{
	//					chronograf.Mapping{
	//						ID:                   "1",
	//						Organization:         "1",
	//						Provider:             "*",
	//						Scheme:               "*",
	//						ProviderOrganization: "influxdata",
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "Sweet",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PUT",
	//				path:   "/chronograf/v1/mappings/1",
	//				payload: &chronograf.Mapping{
	//					ID:                   "1",
	//					Organization:         "1",
	//					Provider:             "*",
	//					Scheme:               "*",
	//					ProviderOrganization: "*",
	//				},
	//				principal: oauth2.Principal{
	//					Subject: "billibob",
	//					Issuer:  "github",
	//					Group:   "influxdata,idk,mimi",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "links": {
	//    "self": "/chronograf/v1/mappings/1"
	//  },
	//  "id": "1",
	//  "organizationId": "1",
	//  "provider": "*",
	//  "scheme": "*",
	//  "providerOrganization": "*"
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /org_config",
	//			subName: "default org",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/org_config",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//			{
	//				"links": {
	//					"self": "\/chronograf\/v1\/org_config",
	//					"logViewer": "\/chronograf\/v1\/org_config\/logviewer"
	//				},
	//				"organization": "default",
	//				"logViewer": {
	//					"columns": [
	//						{
	//							"name": "time",
	//							"position": 0,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "hidden"
	//								}
	//							]
	//						},
	//						{
	//							"name": "severity",
	//							"position": 1,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								},
	//								{
	//									"type": "label",
	//									"value": "icon"
	//								},
	//								{
	//									"type": "label",
	//									"value": "text"
	//								},
	//								{
	//									"type": "color",
	//									"value": "ruby",
	//									"name": "emerg"
	//								},
	//								{
	//									"type": "color",
	//									"value": "fire",
	//									"name": "alert"
	//								},
	//								{
	//									"type": "color",
	//									"value": "curacao",
	//									"name": "crit"
	//								},
	//								{
	//									"type": "color",
	//									"value": "tiger",
	//									"name": "err"
	//								},
	//								{
	//									"type": "color",
	//									"value": "pineapple",
	//									"name": "warning"
	//								},
	//								{
	//									"type": "color",
	//									"value": "rainforest",
	//									"name": "notice"
	//								},
	//								{
	//									"type": "color",
	//									"value": "star",
	//									"name": "info"
	//								},
	//								{
	//									"type": "color",
	//									"value": "wolf",
	//									"name": "debug"
	//								}
	//							]
	//						},
	//						{
	//							"name": "timestamp",
	//							"position": 2,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								}
	//							]
	//						},
	//						{
	//							"name": "message",
	//							"position": 3,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								}
	//							]
	//						},
	//						{
	//							"name": "facility",
	//							"position": 4,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								}
	//							]
	//						},
	//						{
	//							"name": "procid",
	//							"position": 5,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								},
	//								{
	//									"type": "displayName",
	//									"value": "Proc ID"
	//								}
	//							]
	//						},
	//						{
	//							"name": "appname",
	//							"position": 6,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								},
	//								{
	//									"type": "displayName",
	//									"value": "Application"
	//								}
	//							]
	//						},
	//						{
	//							"name": "host",
	//							"position": 7,
	//							"encodings": [
	//								{
	//									"type": "visibility",
	//									"value": "visible"
	//								}
	//							]
	//						}
	//					]
	//				}
	//			}
	//				`,
	//			},
	//		},
	//		{
	//			name:    "GET /org_config/logviewer",
	//			subName: "default org",
	//			fields: fields{
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/org_config/logviewer",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//			{
	//				"links": {
	//					"self": "\/chronograf\/v1\/org_config/logviewer"
	//				},
	//				"columns": [
	//					{
	//						"name": "time",
	//						"position": 0,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "hidden"
	//							}
	//						]
	//					},
	//					{
	//						"name": "severity",
	//						"position": 1,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							},
	//							{
	//								"type": "label",
	//								"value": "icon"
	//							},
	//							{
	//								"type": "label",
	//								"value": "text"
	//							},
	//							{
	//								"type": "color",
	//								"value": "ruby",
	//								"name": "emerg"
	//							},
	//							{
	//								"type": "color",
	//								"value": "fire",
	//								"name": "alert"
	//							},
	//							{
	//								"type": "color",
	//								"value": "curacao",
	//								"name": "crit"
	//							},
	//							{
	//								"type": "color",
	//								"value": "tiger",
	//								"name": "err"
	//							},
	//							{
	//								"type": "color",
	//								"value": "pineapple",
	//								"name": "warning"
	//							},
	//							{
	//								"type": "color",
	//								"value": "rainforest",
	//								"name": "notice"
	//							},
	//							{
	//								"type": "color",
	//								"value": "star",
	//								"name": "info"
	//							},
	//							{
	//								"type": "color",
	//								"value": "wolf",
	//								"name": "debug"
	//							}
	//						]
	//					},
	//					{
	//						"name": "timestamp",
	//						"position": 2,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "message",
	//						"position": 3,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "facility",
	//						"position": 4,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "procid",
	//						"position": 5,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							},
	//							{
	//								"type": "displayName",
	//								"value": "Proc ID"
	//							}
	//						]
	//					},
	//					{
	//						"name": "appname",
	//						"position": 6,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							},
	//							{
	//								"type": "displayName",
	//								"value": "Application"
	//							}
	//						]
	//					},
	//					{
	//						"name": "host",
	//						"position": 7,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					}
	//				]
	//			}
	//				`,
	//			},
	//		},
	//		{
	//			name:    "PUT /org_config/logviewer",
	//			subName: "default org",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "PUT",
	//				path:   "/chronograf/v1/org_config/logviewer",
	//				payload: &chronograf.LogViewerConfig{
	//					Columns: []chronograf.LogViewerColumn{
	//						{
	//							Name:     "time",
	//							Position: 0,
	//							Encodings: []chronograf.ColumnEncoding{
	//								{
	//									Type:  "visibility",
	//									Value: "hidden",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "severity",
	//							Position: 1,
	//							Encodings: []chronograf.ColumnEncoding{
	//
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//								{
	//									Type:  "label",
	//									Value: "icon",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "emerg",
	//									Value: "ruby",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "alert",
	//									Value: "fire",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "crit",
	//									Value: "curacao",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "err",
	//									Value: "tiger",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "warning",
	//									Value: "pineapple",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "notice",
	//									Value: "wolf",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "info",
	//									Value: "wolf",
	//								},
	//								{
	//									Type:  "color",
	//									Name:  "debug",
	//									Value: "wolf",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "timestamp",
	//							Position: 3,
	//							Encodings: []chronograf.ColumnEncoding{
	//
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "message",
	//							Position: 2,
	//							Encodings: []chronograf.ColumnEncoding{
	//
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "facility",
	//							Position: 4,
	//							Encodings: []chronograf.ColumnEncoding{
	//
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "procid",
	//							Position: 5,
	//							Encodings: []chronograf.ColumnEncoding{
	//
	//								{
	//									Type:  "visibility",
	//									Value: "hidden",
	//								},
	//								{
	//									Type:  "displayName",
	//									Value: "ProcID!",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "appname",
	//							Position: 6,
	//							Encodings: []chronograf.ColumnEncoding{
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//								{
	//									Type:  "displayName",
	//									Value: "Application",
	//								},
	//							},
	//						},
	//						{
	//							Name:     "host",
	//							Position: 7,
	//							Encodings: []chronograf.ColumnEncoding{
	//								{
	//									Type:  "visibility",
	//									Value: "visible",
	//								},
	//							},
	//						},
	//					},
	//				},
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//			{
	//				"links": {
	//					"self": "\/chronograf\/v1\/org_config\/logviewer"
	//				},
	//				"columns": [
	//					{
	//						"name": "time",
	//						"position": 0,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "hidden"
	//							}
	//						]
	//					},
	//					{
	//						"name": "severity",
	//						"position": 1,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							},
	//							{
	//								"type": "label",
	//								"value": "icon"
	//							},
	//							{
	//								"type": "color",
	//								"value": "ruby",
	//								"name": "emerg"
	//							},
	//							{
	//								"type": "color",
	//								"value": "fire",
	//								"name": "alert"
	//							},
	//							{
	//								"type": "color",
	//								"value": "curacao",
	//								"name": "crit"
	//							},
	//							{
	//								"type": "color",
	//								"value": "tiger",
	//								"name": "err"
	//							},
	//							{
	//								"type": "color",
	//								"value": "pineapple",
	//								"name": "warning"
	//							},
	//							{
	//								"type": "color",
	//								"value": "wolf",
	//								"name": "notice"
	//							},
	//							{
	//								"type": "color",
	//								"value": "wolf",
	//								"name": "info"
	//							},
	//							{
	//								"type": "color",
	//								"value": "wolf",
	//								"name": "debug"
	//							}
	//						]
	//					},
	//					{
	//						"name": "timestamp",
	//						"position": 3,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "message",
	//						"position": 2,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "facility",
	//						"position": 4,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					},
	//					{
	//						"name": "procid",
	//						"position": 5,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "hidden"
	//							},
	//							{
	//								"type": "displayName",
	//								"value": "ProcID!"
	//							}
	//						]
	//					},
	//					{
	//						"name": "appname",
	//						"position": 6,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							},
	//							{
	//								"type": "displayName",
	//								"value": "Application"
	//							}
	//						]
	//					},
	//					{
	//						"name": "host",
	//						"position": 7,
	//						"encodings": [
	//							{
	//								"type": "visibility",
	//								"value": "visible"
	//							}
	//						]
	//					}
	//				]
	//			}
	//				`,
	//			},
	//		},
	//		{
	//			name:    "GET /",
	//			subName: "signed into default org",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: true,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/",
	//				principal: oauth2.Principal{
	//					Organization: "default",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "layouts": "/chronograf/v1/layouts",
	//  "cells": "/chronograf/v2/cells",
	//  "users": "/chronograf/v1/organizations/default/users",
	//  "allUsers": "/chronograf/v1/users",
	//  "organizations": "/chronograf/v1/organizations",
	//  "mappings": "/chronograf/v1/mappings",
	//  "sources": "/chronograf/v1/sources",
	//  "me": "/chronograf/v1/me",
	//  "environment": "/chronograf/v1/env",
	//  "dashboards": "/chronograf/v1/dashboards",
	//	"dashboardsv2":"/chronograf/v2/dashboards",
	//  "config": {
	//    "self": "/chronograf/v1/config",
	//    "auth": "/chronograf/v1/config/auth"
	//  },
	//  "auth": [
	//    {
	//      "name": "github",
	//      "label": "Github",
	//      "login": "/oauth/github/login",
	//      "logout": "/oauth/github/logout",
	//      "callback": "/oauth/github/callback"
	//    }
	//  ],
	//  "logout": "/oauth/logout",
	//  "external": {
	//    "statusFeed": ""
	//  },
	//  "orgConfig": {
	//    "logViewer": "/chronograf/v1/org_config/logviewer",
	//    "self": "/chronograf/v1/org_config"
	//  },
	//  "flux": {
	//    "ast": "/chronograf/v1/flux/ast",
	//    "self": "/chronograf/v1/flux",
	//    "suggestions": "/chronograf/v1/flux/suggestions"
	//  }
	//}
	//`,
	//			},
	//		},
	//		{
	//			name:    "GET /",
	//			subName: "signed into org 1",
	//			fields: fields{
	//				Config: &chronograf.Config{
	//					Auth: chronograf.AuthConfig{
	//						SuperAdminNewUsers: true,
	//					},
	//				},
	//				Organizations: []chronograf.Organization{
	//					{
	//						ID:          "1",
	//						Name:        "cool",
	//						DefaultRole: roles.ViewerRoleName,
	//					},
	//				},
	//				Users: []chronograf.User{
	//					{
	//						ID:         1, // This is artificial, but should be reflective of the users actual ID
	//						Name:       "billibob",
	//						Provider:   "github",
	//						Scheme:     "oauth2",
	//						SuperAdmin: false,
	//						Roles: []chronograf.Role{
	//							{
	//								Name:         "admin",
	//								Organization: "default",
	//							},
	//							{
	//								Name:         "member",
	//								Organization: "1",
	//							},
	//						},
	//					},
	//				},
	//			},
	//			args: args{
	//				server: &server.Server{
	//					GithubClientID:     "not empty",
	//					GithubClientSecret: "not empty",
	//				},
	//				method: "GET",
	//				path:   "/chronograf/v1/",
	//				principal: oauth2.Principal{
	//					Organization: "1",
	//					Subject:      "billibob",
	//					Issuer:       "github",
	//				},
	//			},
	//			wants: wants{
	//				statusCode: 200,
	//				body: `
	//{
	//  "layouts": "/chronograf/v1/layouts",
	//  "cells": "/chronograf/v2/cells",
	//  "users": "/chronograf/v1/organizations/1/users",
	//  "allUsers": "/chronograf/v1/users",
	//  "organizations": "/chronograf/v1/organizations",
	//  "mappings": "/chronograf/v1/mappings",
	//  "sources": "/chronograf/v1/sources",
	//  "me": "/chronograf/v1/me",
	//  "environment": "/chronograf/v1/env",
	//  "dashboards": "/chronograf/v1/dashboards",
	//	"dashboardsv2":"/chronograf/v2/dashboards",
	//  "config": {
	//    "self": "/chronograf/v1/config",
	//    "auth": "/chronograf/v1/config/auth"
	//  },
	//  "orgConfig": {
	//    "logViewer": "/chronograf/v1/org_config/logviewer",
	//    "self": "/chronograf/v1/org_config"
	//  },
	//  "auth": [
	//    {
	//      "name": "github",
	//      "label": "Github",
	//      "login": "/oauth/github/login",
	//      "logout": "/oauth/github/logout",
	//      "callback": "/oauth/github/callback"
	//    }
	//  ],
	//  "logout": "/oauth/logout",
	//  "external": {
	//    "statusFeed": ""
	//  },
	//  "flux": {
	//    "ast": "/chronograf/v1/flux/ast",
	//    "self": "/chronograf/v1/flux",
	//    "suggestions": "/chronograf/v1/flux/suggestions"
	//  }
	//}
	//`,
	//			},
	//		},
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

			// Endpoint for validating RSA256 signatures when using id_token parsing for ADFS
			tt.args.server.JwksURL = ""

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
			for i, mapping := range tt.fields.Mappings {
				o, err := boltdb.MappingsStore.Add(ctx, &mapping)
				if err != nil {
					t.Fatalf("failed to add mapping: %v", err)
					return
				}
				tt.fields.Mappings[i] = *o
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
			timeout := time.Now().Add(30 * time.Second)
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
			token, _ := oauth2.NewJWT(tt.args.server.TokenSecret, tt.args.server.JwksURL).Create(ctx, tt.args.principal)
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
