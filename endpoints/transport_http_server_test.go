package endpoints_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/endpoints"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/pkg/testttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_HTTPServer(t *testing.T) {
	endpointLinks := func(id influxdb.ID) map[string]interface{} {
		return map[string]interface{}{
			"self":    "/api/v2/notificationEndpoints/" + id.String(),
			"labels":  path.Join("/api/v2/notificationEndpoints/", id.String(), "labels"),
			"members": path.Join("/api/v2/notificationEndpoints/", id.String(), "members"),
			"owners":  path.Join("/api/v2/notificationEndpoints/", id.String(), "owners"),
		}
	}

	t.Run("create endpoint", func(t *testing.T) {
		tests := []struct {
			name     string
			reqBody  map[string]interface{}
			respBody map[string]interface{}
		}{
			{
				name: "http basic",
				reqBody: map[string]interface{}{
					"type":            endpoints.TypeHTTP,
					"authMethod":      "basic",
					"contentTemplate": "templ",
					"description":     "desc",
					"headers": map[string]interface{}{
						"h1": "v1",
					},
					"labels":   []string{"label_1", "label_2"},
					"method":   "GET",
					"name":     "name",
					"orgID":    influxdb.ID(2).String(),
					"status":   string(influxdb.Active),
					"password": "password",
					"url":      "wwww.pagerduty.com/1/2",
					"username": "username",
				},
				respBody: map[string]interface{}{
					"id":              influxdb.ID(1).String(),
					"type":            endpoints.TypeHTTP,
					"authMethod":      "basic",
					"contentTemplate": "templ",
					"description":     "desc",
					"headers": map[string]interface{}{
						"h1": "v1",
					},
					"method": "GET",
					"name":   "name",
					"orgID":  influxdb.ID(2).String(),
					"labels": []interface{}{
						map[string]interface{}{
							"id":   influxdb.ID(1).String(),
							"name": "label_1",
						},
						map[string]interface{}{
							"id":   influxdb.ID(2).String(),
							"name": "label_2",
						},
					},
					"status":    string(influxdb.Active),
					"password":  fmt.Sprintf("secret: %s-password", influxdb.ID(1)),
					"url":       "wwww.pagerduty.com/1/2",
					"username":  fmt.Sprintf("secret: %s-username", influxdb.ID(1)),
					"createdAt": time.Time{}.Format(time.RFC3339),
					"updatedAt": time.Time{}.Format(time.RFC3339),
					"links":     endpointLinks(1),
				},
			},
			{
				name: "pager duty",
				reqBody: map[string]interface{}{
					"type":        endpoints.TypePagerDuty,
					"description": "desc",
					"labels":      []string{"label_1", "label_2"},
					"name":        "name",
					"orgID":       influxdb.ID(2).String(),
					"status":      string(influxdb.Active),
					"routingKey":  "routing-key",
					"clientURL":   "wwww.pagerduty.com/1/2",
				},
				respBody: map[string]interface{}{
					"id":          influxdb.ID(1).String(),
					"type":        endpoints.TypePagerDuty,
					"description": "desc",
					"name":        "name",
					"orgID":       influxdb.ID(2).String(),
					"labels": []interface{}{
						map[string]interface{}{
							"id":   influxdb.ID(1).String(),
							"name": "label_1",
						},
						map[string]interface{}{
							"id":   influxdb.ID(2).String(),
							"name": "label_2",
						},
					},
					"status":     string(influxdb.Active),
					"routingKey": fmt.Sprintf("secret: %s-routing-key", influxdb.ID(1)),
					"clientURL":  "wwww.pagerduty.com/1/2",
					"createdAt":  time.Time{}.Format(time.RFC3339),
					"updatedAt":  time.Time{}.Format(time.RFC3339),
					"links":      endpointLinks(1),
				},
			},
			{
				name: "slack",
				reqBody: map[string]interface{}{
					"type":        endpoints.TypeSlack,
					"description": "desc",
					"labels":      []string{"label_1", "label_2"},
					"name":        "name",
					"orgID":       influxdb.ID(2).String(),
					"status":      string(influxdb.Active),
					"token":       "slack-token-val",
					"url":         "wwww.slackurl.com/1/2",
				},
				respBody: map[string]interface{}{
					"id":          influxdb.ID(1).String(),
					"type":        endpoints.TypeSlack,
					"description": "desc",
					"name":        "name",
					"orgID":       influxdb.ID(2).String(),
					"labels": []interface{}{
						map[string]interface{}{
							"id":   influxdb.ID(1).String(),
							"name": "label_1",
						},
						map[string]interface{}{
							"id":   influxdb.ID(2).String(),
							"name": "label_2",
						},
					},
					"status":    string(influxdb.Active),
					"token":     fmt.Sprintf("secret: %s-token", influxdb.ID(1)),
					"url":       "wwww.slackurl.com/1/2",
					"createdAt": time.Time{}.Format(time.RFC3339),
					"updatedAt": time.Time{}.Format(time.RFC3339),
					"links":     endpointLinks(1),
				},
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				svc := mock.NewNotificationEndpointService()
				svc.CreateF = func(ctx context.Context, userID influxdb.ID, edp influxdb.NotificationEndpoint) error {
					if userID != 13 {
						return errors.New("wrong userID provided; got=" + userID.String())
					}
					base := edp.Base()
					base.ID = 1
					edp.BackfillSecretKeys()
					for i := range base.Labels {
						base.Labels[i].ID = influxdb.ID(i + 1)
					}
					return nil
				}

				svr := endpoints.NewHTTPServer(nil, svc)

				testttp.
					PostJSON(t, "/api/v2/notificationEndpoints", tt.reqBody).
					WrapCtx(authCtxFn(13)).
					Do(svr).
					ExpectStatus(http.StatusCreated).
					ExpectBody(func(body *bytes.Buffer) {
						var m map[string]interface{}
						require.NoError(t, json.NewDecoder(body).Decode(&m))
						assert.Equal(t, tt.respBody, m)
					})
			}
			t.Run(tt.name, fn)
		}
	})
}

func authCtxFn(userID influxdb.ID) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return pcontext.SetAuthorizer(ctx, &influxdb.Session{UserID: userID})
	}
}
