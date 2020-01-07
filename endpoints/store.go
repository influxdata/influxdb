package endpoints

import (
	"context"

	"github.com/influxdata/influxdb/notification/endpoint"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
)

type StoreKV struct {
	kvStore  kv.Store
	idxStore *kv.IndexStore
}

var _ Store = (*StoreKV)(nil)

func NewStore(kvStore kv.Store) *StoreKV {
	var decEndpointEntFn kv.DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		edp, err := endpoint.UnmarshalJSON(val)
		return key, edp, err
	}

	var decValToEntFn kv.ConvertValToEntFn = func(_ []byte, v interface{}) (kv.Entity, error) {
		edp, ok := v.(influxdb.NotificationEndpoint)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}
		return kv.Entity{
			PK:        kv.EncID(edp.GetID()),
			UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
			Body:      edp,
		}, nil
	}

	idxStore := kv.IndexStore{
		Resource:   serviceName,
		EntStore:   kv.NewStoreBase(serviceName, []byte("notificationEndpointv1"), kv.EncIDKey, kv.EncBodyJSON, decEndpointEntFn, decValToEntFn),
		IndexStore: kv.NewOrgNameKeyStore(serviceName, []byte("notificationEndpointIndexv1"), false),
	}

	return &StoreKV{
		kvStore:  kvStore,
		idxStore: &idxStore,
	}
}

func (s *StoreKV) Init(ctx context.Context) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.idxStore.Init(ctx, tx)
	})
}

func (s *StoreKV) Create(ctx context.Context, edp influxdb.NotificationEndpoint) error {
	return s.put(ctx, edp, kv.PutNew())
}

func (s *StoreKV) Delete(ctx context.Context, id influxdb.ID) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.idxStore.DeleteEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	})
}

type FindFilter struct {
	UserMappings map[influxdb.ID]bool

	OrgID influxdb.ID
	Name  string

	Descending bool
	Offset     int
	Limit      int
}

func (f FindFilter) filter() kv.FilterFn {
	return func(key []byte, val interface{}) bool {
		edp := val.(influxdb.NotificationEndpoint)
		if f.OrgID != 0 && edp.GetOrgID() != f.OrgID {
			return false
		}

		if f.Name != "" && edp.GetName() != f.Name {
			return false
		}

		if f.UserMappings == nil {
			return true
		}
		return f.UserMappings[edp.GetID()]
	}
}

func (s *StoreKV) Find(ctx context.Context, f FindFilter) ([]influxdb.NotificationEndpoint, error) {
	var endpoints []influxdb.NotificationEndpoint
	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		return s.idxStore.Find(ctx, tx, kv.FindOpts{
			Descending:  f.Descending,
			Offset:      f.Offset,
			Limit:       f.Limit,
			FilterEntFn: f.filter(),
			CaptureFn: func(k []byte, v interface{}) error {
				edp, ok := v.(influxdb.NotificationEndpoint)
				if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
					return err
				}
				endpoints = append(endpoints, edp)
				return nil
			},
		})
	})
	if err != nil {
		return nil, err
	}
	return endpoints, nil
}

func (s *StoreKV) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	var edp influxdb.NotificationEndpoint
	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		decodedEnt, err := s.idxStore.FindEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
		if err != nil {
			return err
		}
		edp = decodedEnt.(influxdb.NotificationEndpoint)
		return nil
	})
	return edp, err
}

// Put is unused by Store interface, but is useful in tests for dumping endpoints in without
// worrying about all the other goodies around it.
func (s *StoreKV) Put(ctx context.Context, edp influxdb.NotificationEndpoint) error {
	return s.put(ctx, edp)
}

func (s *StoreKV) Update(ctx context.Context, edp influxdb.NotificationEndpoint) error {
	return s.put(ctx, edp, kv.PutUpdate())
}

func (s *StoreKV) put(ctx context.Context, edp influxdb.NotificationEndpoint, opts ...kv.PutOptionFn) error {
	ent := kv.Entity{
		PK:        kv.EncID(edp.GetID()),
		UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
		Body:      edp,
	}
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.idxStore.Put(ctx, tx, ent, opts...)
	})
}
