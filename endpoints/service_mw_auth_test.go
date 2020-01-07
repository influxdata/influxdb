package endpoints_test

import (
	"context"
	"errors"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/endpoints"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/stretchr/testify/require"
)

func Test_MiddlewareAuth(t *testing.T) {
	newDefaultAuthAgent := func(id, expectedOrgID influxdb.ID, expectedAction influxdb.Action) *mock.AuthAgent {
		authAgent := mock.NewAuthAgent()
		authAgent.IsWritableFn = func(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error {
			if resType != influxdb.NotificationEndpointResourceType {
				return errors.New("wrong resoruce type: " + string(resType))
			}
			if id > 100 {
				return errors.New("unauthed")
			}
			return nil
		}
		authAgent.OrgPermissionFn = func(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error {
			if action != expectedAction {
				return errors.New("wrongo action")
			}
			if orgID != expectedOrgID {
				return errors.New("wrongo org id")
			}
			return nil
		}
		return authAgent
	}

	t.Run("Delete", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			svc := mock.NewNotificationEndpointService()
			svc.FindByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
				e := &endpoint.HTTP{}
				e.SetID(id)
				e.SetOrgID(2)
				return e, nil
			}

			authAgent := newDefaultAuthAgent(1, 2, influxdb.WriteAction)
			endpointSVC := endpoints.MiddlewareAuth(authAgent)(svc)

			err := endpointSVC.Delete(context.TODO(), 1)
			require.NoError(t, err)
		})

		t.Run("error cases", func(t *testing.T) {
			t.Run("not authorized for org", func(t *testing.T) {
				svc := mock.NewNotificationEndpointService()
				svc.FindByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
					e := &endpoint.HTTP{}
					e.SetID(id)
					badOrgID := influxdb.ID(3000)
					e.SetOrgID(badOrgID)
					return e, nil
				}

				authAgent := newDefaultAuthAgent(1, 2, influxdb.WriteAction)
				endpointSVC := endpoints.MiddlewareAuth(authAgent)(svc)

				err := endpointSVC.Delete(context.TODO(), 1)
				require.Error(t, err)
			})
		})
	})
}
