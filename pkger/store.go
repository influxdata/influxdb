package pkger

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

type (
	entStack struct {
		ID    []byte `json:"id"`
		OrgID []byte `json:"orgID"`

		Name        string             `json:"name"`
		Description string             `json:"description"`
		Sources     []string           `json:"sources,omitempty"`
		URLs        []string           `json:"urls,omitempty"`
		Resources   []entStackResource `json:"resources,omitempty"`

		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	entStackResource struct {
		APIVersion   string                `json:"apiVersion"`
		ID           string                `json:"id"`
		Kind         string                `json:"kind"`
		Name         string                `json:"name"`
		Associations []entStackAssociation `json:"associations,omitempty"`
	}

	entStackAssociation struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
)

// StoreKV is a store implementation that uses a kv store backing.
type StoreKV struct {
	kvStore   kv.Store
	indexBase *kv.IndexStore

	once sync.Once
}

var _ Store = (*StoreKV)(nil)

// NewStoreKV creates a new StoreKV entity. This does not initialize the store. You will
// want to init it if you want to have this init donezo at startup. If not it'll lazy
// load the buckets as they are used.
func NewStoreKV(store kv.Store) *StoreKV {
	const resource = "stack"

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
func (s *StoreKV) CreateStack(ctx context.Context, stack Stack) error {
	return s.put(ctx, stack, kv.PutNew())
}

// ListStacks returns a list of stacks.
func (s *StoreKV) ListStacks(ctx context.Context, orgID influxdb.ID, f ListFilter) ([]Stack, error) {
	if len(f.StackIDs) > 0 && len(f.Names) == 0 {
		return s.listStacksByID(ctx, orgID, f.StackIDs)
	}

	filterFn, err := storeListFilterFn(orgID, f)
	if err != nil {
		return nil, err
	}

	var stacks []Stack
	err = s.view(ctx, func(tx kv.Tx) error {
		return s.indexBase.Find(ctx, tx, kv.FindOpts{
			CaptureFn: func(key []byte, decodedVal interface{}) error {
				stack, err := convertStackEntToStack(decodedVal.(*entStack))
				if err != nil {
					return err
				}
				stacks = append(stacks, stack)
				return nil
			},
			FilterEntFn: func(key []byte, decodedVal interface{}) bool {
				st := decodedVal.(*entStack)
				return filterFn(st)
			},
		})
	})
	if err != nil {
		return nil, err
	}
	return stacks, nil
}

func storeListFilterFn(orgID influxdb.ID, f ListFilter) (func(*entStack) bool, error) {
	orgIDEncoded, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	mIDs := make(map[string]bool)
	for _, id := range f.StackIDs {
		b, err := id.Encode()
		if err != nil {
			return nil, err
		}
		mIDs[string(b)] = true
	}

	mNames := make(map[string]bool)
	for _, name := range f.Names {
		mNames[name] = true
	}

	optionalFieldFilterFn := func(ent *entStack) bool {
		if len(mIDs) > 0 || len(mNames) > 0 {
			return mIDs[string(ent.ID)] || mNames[ent.Name]
		}
		return true
	}
	return func(st *entStack) bool {
		return bytes.Equal(orgIDEncoded, st.OrgID) && optionalFieldFilterFn(st)
	}, nil
}

func (s *StoreKV) listStacksByID(ctx context.Context, orgID influxdb.ID, stackIDs []influxdb.ID) ([]Stack, error) {
	var stacks []Stack
	for _, id := range stackIDs {
		st, err := s.ReadStackByID(ctx, id)
		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			// since the stackIDs are a filter, if it is not found, we just continue
			// on. If the user wants to verify the existence of a particular stack
			// then it would be upon them to use the ReadByID call.
			continue
		}
		if err != nil {
			return nil, err
		}
		if orgID != st.OrgID {
			continue
		}
		stacks = append(stacks, st)
	}
	return stacks, nil
}

// ReadStackByID reads a stack by the provided ID.
func (s *StoreKV) ReadStackByID(ctx context.Context, id influxdb.ID) (Stack, error) {
	var stack Stack
	err := s.view(ctx, func(tx kv.Tx) error {
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
func (s *StoreKV) UpdateStack(ctx context.Context, stack Stack) error {
	existing, err := s.ReadStackByID(ctx, stack.ID)
	if err != nil {
		return err
	}

	if stack.OrgID != existing.OrgID {
		return &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "org id does not match",
		}
	}

	return s.put(ctx, stack, kv.PutUpdate())
}

// DeleteStack deletes a stack by id.
func (s *StoreKV) DeleteStack(ctx context.Context, id influxdb.ID) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.indexBase.DeleteEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	})
}

func (s *StoreKV) put(ctx context.Context, stack Stack, opts ...kv.PutOptionFn) error {
	ent, err := convertStackToEnt(stack)
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

func (s *StoreKV) view(ctx context.Context, fn func(tx kv.Tx) error) error {
	if err := s.lazyInit(ctx); err != nil {
		return err
	}
	return s.kvStore.View(ctx, fn)
}

func (s *StoreKV) lazyInit(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		err = s.Init(ctx)
	})
	return err
}

func convertStackToEnt(stack Stack) (kv.Entity, error) {
	idBytes, err := stack.ID.Encode()
	if err != nil {
		return kv.Entity{}, err
	}

	orgIDBytes, err := stack.OrgID.Encode()
	if err != nil {
		return kv.Entity{}, err
	}

	stEnt := entStack{
		ID:          idBytes,
		OrgID:       orgIDBytes,
		Name:        stack.Name,
		Description: stack.Description,
		CreatedAt:   stack.CreatedAt,
		UpdatedAt:   stack.UpdatedAt,
		Sources:     stack.Sources,
		URLs:        stack.URLs,
	}

	for _, res := range stack.Resources {
		var associations []entStackAssociation
		for _, ass := range res.Associations {
			associations = append(associations, entStackAssociation{
				Kind: ass.Kind.String(),
				Name: ass.PkgName,
			})
		}
		stEnt.Resources = append(stEnt.Resources, entStackResource{
			APIVersion:   res.APIVersion,
			ID:           res.ID.String(),
			Kind:         res.Kind.String(),
			Name:         res.MetaName,
			Associations: associations,
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
		Name:        ent.Name,
		Description: ent.Description,
		Sources:     ent.Sources,
		URLs:        ent.URLs,
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: ent.CreatedAt,
			UpdatedAt: ent.UpdatedAt,
		},
	}
	if err := stack.ID.Decode(ent.ID); err != nil {
		return Stack{}, err
	}

	if err := stack.OrgID.Decode(ent.OrgID); err != nil {
		return Stack{}, err
	}

	for _, res := range ent.Resources {
		stackRes := StackResource{
			APIVersion: res.APIVersion,
			Kind:       Kind(res.Kind),
			MetaName:   res.Name,
		}
		if err := stackRes.ID.DecodeFromString(res.ID); err != nil {
			return Stack{}, nil
		}

		for _, ass := range res.Associations {
			stackRes.Associations = append(stackRes.Associations, StackResourceAssociation{
				Kind:    Kind(ass.Kind),
				PkgName: ass.Name,
			})
		}

		stack.Resources = append(stack.Resources, stackRes)
	}

	return stack, nil
}
