package kv

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
)

func newEndpointStore() *IndexStore {
	const resource = "notification endpoint"

	var decEndpointEntFn DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		edp, err := endpoint.UnmarshalJSON(val)
		return key, edp, err
	}

	var decValToEntFn ConvertValToEntFn = func(_ []byte, v interface{}) (Entity, error) {
		edp, ok := v.(influxdb.NotificationEndpoint)
		if err := IsErrUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}
		return Entity{
			PK:        EncID(edp.GetID()),
			UniqueKey: Encode(EncID(edp.GetOrgID()), EncString(edp.GetName())),
			Body:      edp,
		}, nil
	}

	return &IndexStore{
		Resource:   resource,
		EntStore:   NewStoreBase(resource, []byte("notificationEndpointv1"), EncIDKey, EncBodyJSON, decEndpointEntFn, decValToEntFn),
		IndexStore: NewOrgNameKeyStore(resource, []byte("notificationEndpointIndexv1"), false),
	}
}

func (s *Service) findNotificationEndpointByID(ctx context.Context, tx Tx, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	decodedEnt, err := s.endpointStore.FindEnt(ctx, tx, Entity{PK: EncID(id)})
	if err != nil {
		return nil, err
	}
	edp, ok := decodedEnt.(influxdb.NotificationEndpoint)
	return edp, IsErrUnexpectedDecodeVal(ok)
}
