package pkger

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
)

type (
	entStack struct {
		ID    []byte `json:"id"`
		OrgID []byte `json:"orgID"`

		URLs      []string           `json:"urls,omitempty"`
		Resources []entStackResource `json:"resources,omitempty"`

		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	entStackResource struct {
		APIVersion string `json:"apiVersion"`
		ID         string `json:"id"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
	}
)

// StoreKV is a store implementation that uses a kv store backing.
type StoreKV struct {
	kvStore   kv.Store
	indexBase *kv.IndexStore
}

var _ Store = (*StoreKV)(nil)

// NewStoreKV creates a new StoreKV entity. This does not initialize the store. You will
// want to init it if you want to have this init donezo at startup. If not it'll lazy
// load the buckets as they are used.
func NewStoreKV(store kv.Store) *StoreKV {
	const resource = "pkg stack"

	storeKV := &StoreKV{
		kvStore: store,
	}
	storeKV.indexBase = &kv.IndexStore{
		Resource:   resource,
		EntStore:   storeKV.entStoreBase(resource),
		IndexStore: storeKV.indexStoreBase(resource),
	}
	return storeKV
}

// Init will initialize the all required buckets for the kv store. If not called, will be
// called implicitly on first read/write operation.
func (s *StoreKV) Init(ctx context.Context) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.indexBase.Init(ctx, tx)
	})
}

// CreateStack will create a new stack. If collisions are found will fail.
func (s *StoreKV) CreateStack(ctx context.Context, orgID influxdb.ID, stack Stack) error {
	return s.put(ctx, orgID, stack, kv.PutNew())
}

// ReadStackByID reads a stack by the provided ID.
func (s *StoreKV) ReadStackByID(ctx context.Context, id influxdb.ID) (Stack, error) {
	var stack Stack
	err := s.kvStore.View(ctx, func(tx kv.Tx) error {
		decodedEnt, err := s.indexBase.FindEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
		if err != nil {
			return err
		}
		stack, err = convertStackEntToStack(decodedEnt.(*entStack))
		return err
	})
	return stack, err
}

// UpdateStack updates a stack.
func (s *StoreKV) UpdateStack(ctx context.Context, orgID influxdb.ID, stack Stack) error {
	return s.put(ctx, orgID, stack, kv.PutUpdate())
}

// DeleteStack delets a stack by id.
func (s *StoreKV) DeleteStack(ctx context.Context, id influxdb.ID) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.indexBase.DeleteEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	})
}

func (s *StoreKV) put(ctx context.Context, orgID influxdb.ID, stack Stack, opts ...kv.PutOptionFn) error {
	ent, err := convertStackToEnt(orgID, stack)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.indexBase.Put(ctx, tx, ent, opts...)
	})
}

func (s *StoreKV) entStoreBase(resource string) *kv.StoreBase {
	var decodeEntFn kv.DecodeBucketValFn = func(key, val []byte) (keyRepeat []byte, decodedVal interface{}, err error) {
		var stack entStack
		return key, &stack, json.Unmarshal(val, &stack)
	}

	var decValToEntFn kv.ConvertValToEntFn = func(k []byte, i interface{}) (kv.Entity, error) {
		s, ok := i.(*entStack)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}

		return kv.Entity{
			PK:        kv.EncBytes(s.ID),
			UniqueKey: kv.Encode(kv.EncBytes(s.OrgID), kv.EncBytes(s.ID)),
			Body:      s,
		}, nil
	}

	entityBucket := []byte("v1_pkger_stacks")

	return kv.NewStoreBase(resource, entityBucket, kv.EncIDKey, kv.EncBodyJSON, decodeEntFn, decValToEntFn)
}

func (s *StoreKV) indexStoreBase(resource string) *kv.StoreBase {
	var decValToEntFn kv.ConvertValToEntFn = func(k []byte, v interface{}) (kv.Entity, error) {
		id, ok := v.(influxdb.ID)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}

		return kv.Entity{
			PK:        kv.EncID(id),
			UniqueKey: kv.EncBytes(k),
		}, nil
	}

	indexBucket := []byte("v1_pkger_stacks_index")

	return kv.NewStoreBase(resource, indexBucket, kv.EncUniqKey, kv.EncIDKey, kv.DecIndexID, decValToEntFn)
}

func convertStackToEnt(orgID influxdb.ID, stack Stack) (kv.Entity, error) {
	idBytes, err := stack.ID.Encode()
	if err != nil {
		return kv.Entity{}, err
	}

	orgIDBytes, err := orgID.Encode()
	if err != nil {
		return kv.Entity{}, err
	}

	urlStrs := make([]string, 0, len(stack.URLS))
	for _, u := range stack.URLS {
		urlStrs = append(urlStrs, u.String())
	}

	stEnt := entStack{
		ID:        idBytes,
		OrgID:     orgIDBytes,
		CreatedAt: stack.CreatedAt,
		UpdatedAt: stack.UpdatedAt,
		URLs:      urlStrs,
	}

	for _, res := range stack.Resources {
		stEnt.Resources = append(stEnt.Resources, entStackResource{
			APIVersion: res.APIVersion,
			ID:         res.ID.String(),
			Kind:       res.Kind.String(),
			Name:       res.Name,
		})
	}

	return kv.Entity{
		PK:        kv.EncBytes(stEnt.ID),
		UniqueKey: kv.Encode(kv.EncBytes(stEnt.OrgID), kv.EncBytes(stEnt.ID)),
		Body:      stEnt,
	}, nil
}

func convertStackEntToStack(ent *entStack) (Stack, error) {
	stack := Stack{
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: ent.CreatedAt,
			UpdatedAt: ent.UpdatedAt,
		},
	}
	if err := stack.ID.Decode(ent.ID); err != nil {
		return Stack{}, err
	}

	for _, urlStr := range ent.URLs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return Stack{}, err
		}
		stack.URLS = append(stack.URLS, *u)
	}

	for _, res := range ent.Resources {
		stackRes := StackResource{
			APIVersion: res.APIVersion,
			Kind:       Kind(res.Kind),
			Name:       res.Name,
		}
		if err := stackRes.ID.DecodeFromString(res.ID); err != nil {
			return Stack{}, nil
		}

		stack.Resources = append(stack.Resources, stackRes)
	}

	return stack, nil
}
