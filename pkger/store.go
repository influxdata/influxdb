package pkger

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
)

type (
	entStack struct {
		ID        []byte    `json:"id"`
		OrgID     []byte    `json:"orgID"`
		CreatedAt time.Time `json:"createdAt"`

		Events []entStackEvent `json:"events"`

		// this embedding is for stacks that were
		// created before events, this should stay
		// for some time.
		entStackEvent
	}

	entStackEvent struct {
		EventType   StackEventType     `json:"eventType"`
		Name        string             `json:"name"`
		Description string             `json:"description"`
		Sources     []string           `json:"sources,omitempty"`
		URLs        []string           `json:"urls,omitempty"`
		Resources   []entStackResource `json:"resources,omitempty"`
		UpdatedAt   time.Time          `json:"updatedAt"`
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

// CreateStack will create a new stack. If collisions are found will fail.
func (s *StoreKV) CreateStack(ctx context.Context, stack Stack) error {
	return s.put(ctx, stack, kv.PutNew())
}

// ListStacks returns a list of stacks.
func (s *StoreKV) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
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

func storeListFilterFn(orgID platform.ID, f ListFilter) (func(*entStack) bool, error) {
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
		switch {
		case mIDs[string(ent.ID)]:
			return true
		// existing data before stacks are event sourced have
		// this shape.
		case len(mNames) > 0 && ent.Name != "":
			return mNames[ent.Name]
		case len(mNames) > 0 && len(ent.Events) > 0:
			sort.Slice(ent.Events, func(i, j int) bool {
				return ent.Events[i].UpdatedAt.After(ent.Events[j].UpdatedAt)
			})
			return mNames[ent.Events[0].Name]
		}
		return true
	}
	return func(st *entStack) bool {
		return bytes.Equal(orgIDEncoded, st.OrgID) && optionalFieldFilterFn(st)
	}, nil
}

func (s *StoreKV) listStacksByID(ctx context.Context, orgID platform.ID, stackIDs []platform.ID) ([]Stack, error) {
	var stacks []Stack
	for _, id := range stackIDs {
		st, err := s.ReadStackByID(ctx, id)
		if errors.ErrorCode(err) == errors.ENotFound {
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
func (s *StoreKV) ReadStackByID(ctx context.Context, id platform.ID) (Stack, error) {
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
		return &errors.Error{
			Code: errors.EUnprocessableEntity,
			Msg:  "org id does not match",
		}
	}

	return s.put(ctx, stack, kv.PutUpdate())
}

// DeleteStack deletes a stack by id.
func (s *StoreKV) DeleteStack(ctx context.Context, id platform.ID) error {
	return s.kvStore.Update(ctx, func(tx kv.Tx) error {
		return s.indexBase.DeleteEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	})
}

func (s *StoreKV) put(ctx context.Context, stack Stack, opts ...kv.PutOptionFn) error {
	ent, err := convertStackToEnt(stack)
	if err != nil {
		return influxErr(errors.EInvalid, err)
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
		id, ok := v.(platform.ID)
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
	return s.kvStore.View(ctx, fn)
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
		ID:        idBytes,
		OrgID:     orgIDBytes,
		CreatedAt: stack.CreatedAt,
	}
	for _, ev := range stack.Events {
		var resources []entStackResource
		for _, res := range ev.Resources {
			var associations []entStackAssociation
			for _, ass := range res.Associations {
				associations = append(associations, entStackAssociation{
					Kind: ass.Kind.String(),
					Name: ass.MetaName,
				})
			}
			resources = append(resources, entStackResource{
				APIVersion:   res.APIVersion,
				ID:           res.ID.String(),
				Kind:         res.Kind.String(),
				Name:         res.MetaName,
				Associations: associations,
			})
		}
		stEnt.Events = append(stEnt.Events, entStackEvent{
			EventType:   ev.EventType,
			Name:        ev.Name,
			Description: ev.Description,
			Sources:     ev.Sources,
			URLs:        ev.TemplateURLs,
			Resources:   resources,
			UpdatedAt:   ev.UpdatedAt,
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
		CreatedAt: ent.CreatedAt,
	}
	if err := stack.ID.Decode(ent.ID); err != nil {
		return Stack{}, err
	}

	if err := stack.OrgID.Decode(ent.OrgID); err != nil {
		return Stack{}, err
	}

	entEvents := ent.Events

	// ensure backwards compatibility. All existing fields
	// will be associated with a createEvent, regardless if
	// they are or not
	if !ent.UpdatedAt.IsZero() {
		entEvents = append(entEvents, ent.entStackEvent)
	}

	for _, entEv := range entEvents {
		ev, err := convertEntStackEvent(entEv)
		if err != nil {
			return Stack{}, err
		}
		stack.Events = append(stack.Events, ev)
	}

	return stack, nil
}

func convertEntStackEvent(ent entStackEvent) (StackEvent, error) {
	ev := StackEvent{
		EventType:    ent.EventType,
		Name:         ent.Name,
		Description:  ent.Description,
		Sources:      ent.Sources,
		TemplateURLs: ent.URLs,
		UpdatedAt:    ent.UpdatedAt,
	}
	out, err := convertStackEntResources(ent.Resources)
	if err != nil {
		return StackEvent{}, err
	}
	ev.Resources = out
	return ev, nil
}

func convertStackEntResources(entResources []entStackResource) ([]StackResource, error) {
	var out []StackResource
	for _, res := range entResources {
		stackRes := StackResource{
			APIVersion: res.APIVersion,
			Kind:       Kind(res.Kind),
			MetaName:   res.Name,
		}
		if err := stackRes.ID.DecodeFromString(res.ID); err != nil {
			return nil, err
		}

		for _, ass := range res.Associations {
			stackRes.Associations = append(stackRes.Associations, StackResourceAssociation{
				Kind:     Kind(ass.Kind),
				MetaName: ass.Name,
			})
		}
		out = append(out, stackRes)
	}
	return out, nil
}
