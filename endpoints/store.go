package endpoints

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/notification/endpoint"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
)

type (
	// why have another type for the store? to remove the concerns with encoding
	// marshal/unmarshal from the domain. Makes things very difficult to
	// extend any further.
	entity struct {
		// Common fields
		ID          string `json:"id"`
		Type        string `json:"type"`
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
		OrgID       string `json:"orgID,omitempty"`
		Status      string `json:"status"`
		Token       string `json:"token,omitempty"`
		URL         string `json:"url"`

		// CRUD fields
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`

		// specifics
		entityHTTP
		entityPagerDuty
	}

	entityHTTP struct {
		AuthMethod      string            `json:"authMethod,omitempty"`
		ContentTemplate string            `json:"contentTemplate,omitempty"`
		Headers         map[string]string `json:"headers,omitempty"`
		Method          string            `json:"method,omitempty"`
		Password        string            `json:"password,omitempty"`
		Username        string            `json:"username,omitempty"`
	}

	entityPagerDuty struct {
		ClientURL  string `json:"clientURL,omitempty"`
		RoutingKey string `json:"routingKey,omitempty"`
	}
)

type StoreKV struct {
	kvStore  kv.Store
	idxStore *kv.IndexStore
}

var _ Store = (*StoreKV)(nil)

func NewStore(kvStore kv.Store) *StoreKV {
	var decEndpointEntFn kv.DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		var ent entity
		if err := json.Unmarshal(val, &ent); err != nil {
			return key, nil, err
		}
		edp, err := convertEntToEndpoint(ent)
		return key, edp, err
	}

	var decValToEntFn kv.ConvertValToEntFn = func(_ []byte, v interface{}) (kv.Entity, error) {
		edp, ok := v.(influxdb.NotificationEndpoint)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}

		base := edp.Base()
		return kv.Entity{
			PK:        kv.EncID(base.ID),
			UniqueKey: kv.Encode(kv.EncID(base.OrgID), kv.EncString(base.Name)),
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
		base := val.(influxdb.NotificationEndpoint).Base()
		if f.OrgID != 0 && base.OrgID != f.OrgID {
			return false
		}

		if f.Name != "" && base.Name != f.Name {
			return false
		}

		if f.UserMappings == nil {
			return true
		}
		return f.UserMappings[base.ID]
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
	base := edp.Base()

	ent := kv.Entity{
		PK:        kv.EncID(base.ID),
		UniqueKey: kv.Encode(kv.EncID(base.OrgID), kv.EncString(base.Name)),
		Body:      convertEndpointToEntity(edp),
	}
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.idxStore.Put(ctx, tx, ent, opts...)
	})
}

func convertEndpointToEntity(edp influxdb.NotificationEndpoint) entity {
	base := edp.Base()

	ent := entity{
		ID:          base.ID.String(),
		Type:        edp.Type(),
		Name:        base.Name,
		Description: base.Description,
		OrgID:       base.OrgID.String(),
		Status:      string(base.Status),
		CreatedAt:   base.CreatedAt,
		UpdatedAt:   base.UpdatedAt,
	}

	switch t := edp.(type) {
	case *endpoint.HTTP:
		ent.URL = t.URL
		ent.Token = convertSecret(t.Token)
		ent.entityHTTP = entityHTTP{
			AuthMethod:      t.AuthMethod,
			ContentTemplate: t.ContentTemplate,
			Headers:         t.Headers,
			Method:          t.Method,
			Password:        convertSecret(t.Password),
			Username:        convertSecret(t.Username),
		}
	case *endpoint.PagerDuty:
		ent.entityPagerDuty = entityPagerDuty{
			ClientURL:  t.ClientURL,
			RoutingKey: convertSecret(t.RoutingKey),
		}
	case *endpoint.Slack:
		ent.URL = t.URL
		ent.Token = convertSecret(t.Token)
	}

	return ent
}

func convertEntToEndpoint(ent entity) (influxdb.NotificationEndpoint, error) {
	base := influxdb.EndpointBase{
		Name:        ent.Name,
		Description: ent.Description,
		Status:      influxdb.Status(ent.Status),
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: ent.CreatedAt,
			UpdatedAt: ent.UpdatedAt,
		},
	}

	id, err := influxdb.IDFromString(ent.ID)
	if err != nil {
		return nil, err
	}
	base.ID = *id

	orgID, err := influxdb.IDFromString(ent.OrgID)
	if err != nil {
		return nil, err
	}
	base.OrgID = *orgID

	switch ent.Type {
	case TypeHTTP:
		return &endpoint.HTTP{
			EndpointBase:    base,
			URL:             ent.URL,
			Headers:         ent.Headers,
			Token:           convertEntSecret(ent.Token),
			Username:        convertEntSecret(ent.Username),
			Password:        convertEntSecret(ent.Password),
			AuthMethod:      ent.AuthMethod,
			Method:          ent.Method,
			ContentTemplate: ent.ContentTemplate,
		}, nil
	case TypePagerDuty:
		return &endpoint.PagerDuty{
			EndpointBase: base,
			ClientURL:    ent.ClientURL,
			RoutingKey:   convertEntSecret(ent.RoutingKey),
		}, nil
	case TypeSlack:
		return &endpoint.Slack{
			EndpointBase: base,
			URL:          ent.URL,
			Token:        convertEntSecret(ent.Token),
		}, nil
	default:
		return nil, &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  "invalid endpoint type provided: " + ent.Type,
		}
	}
}

func convertSecret(secret influxdb.SecretField) string {
	return secret.Key
}

func convertEntSecret(sec string) influxdb.SecretField {
	return influxdb.SecretField{Key: sec}
}
