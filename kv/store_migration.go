package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	ierrors "github.com/influxdata/influxdb/kit/errors"

	"github.com/influxdata/influxdb/kit/tracing"

	"github.com/influxdata/influxdb"
)

type Migration struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	RunAt       time.Time `json:"runAt"`

	Up func(ctx context.Context, tx Tx, store *IndexStore) error `json:"-"`
}

type MigrationIndexStore struct {
	migStore *StoreBase

	*IndexStore
}

func NewMigrationIndexStore(indexStore *IndexStore) *MigrationIndexStore {
	var encEntKey EncodeEntFn = func(ent Entity) ([]byte, string, error) {
		mig, ok := ent.Body.(Migration)
		if err := errUnexpectedDecodeVal(ok); err != nil {
			return nil, "body", err
		}
		if mig.Name == "" {
			return nil, "body", errors.New("no migration name provided")
		}
		return []byte(mig.Name), "body", nil
	}

	var decEntVal DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		var mig Migration
		if err := json.Unmarshal(val, &mig); err != nil {
			return nil, nil, &influxdb.Error{Code: influxdb.EInternal, Err: err}
		}
		fmt.Print(mig)
		return key, mig, nil
	}

	var decodedValToEnt ConvertValToEntFn = func(k []byte, v interface{}) (Entity, error) {
		mig, ok := v.(Migration)
		if err := errUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}
		return Entity{Body: mig}, nil
	}

	bktName := []byte(string(indexStore.EntStore.BktName) + "_migrations")
	return &MigrationIndexStore{
		IndexStore: indexStore,
		migStore:   NewStoreBase(indexStore.Resource, bktName, encEntKey, EncBodyJSON, decEntVal, decodedValToEnt),
	}
}

func (s *MigrationIndexStore) Init(ctx context.Context, tx Tx) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.EntStore.Init(ctx, tx); err != nil {
		return err
	}
	return s.IndexStore.Init(ctx, tx)
}

func (s *MigrationIndexStore) Up(ctx context.Context, tx Tx, migrations ...Migration) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if len(migrations) == 0 {
		return nil
	}

	existingMigs, err := s.List(ctx, tx)
	if err != nil {
		return err
	}

	mMigs := make(map[string]Migration)
	var errs []string
	for _, mig := range migrations {
		if _, ok := mMigs[mig.Name]; ok {
			errs = append(errs, fmt.Sprintf("duplicate migrations provided for name: %s", mig.Name))
			continue
		}
		mMigs[mig.Name] = mig
	}
	if len(errs) > 0 {
		return &influxdb.Error{Code: influxdb.EConflict, Msg: strings.Join(errs, "\n")}
	}

	for _, existing := range existingMigs {
		delete(mMigs, existing.Name)
	}

	for _, mig := range mMigs {
		if err := s.apply(ctx, tx, mig); err != nil {
			return err
		}
	}
	return nil
}

func (s *MigrationIndexStore) apply(ctx context.Context, tx Tx, mig Migration) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	mig.RunAt = time.Now()

	if err := s.migStore.Put(ctx, tx, Entity{Body: mig}); err != nil {
		return err
	}

	if err := mig.Up(ctx, tx, s.IndexStore); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("migration %q failed", mig.Name),
			Err:  err,
		}
	}
	return nil
}

func (s *MigrationIndexStore) List(ctx context.Context, tx Tx) ([]Migration, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var migs []Migration
	err := s.migStore.Find(ctx, tx, FindOpts{
		Descending: true,
		CaptureFn: func(key []byte, decodedVal interface{}) error {
			migs = append(migs, decodedVal.(Migration))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return migs, nil
}

// MigrationSyncIndexStore adds an index entry for all entities.
func MigrationSyncIndexStore(ctx context.Context, tx Tx, store *IndexStore) error {
	return store.EntStore.Find(ctx, tx, FindOpts{
		CaptureFn: func(key []byte, decodedVal interface{}) error {
			ent, err := store.EntStore.ConvertValToEntFn(key, decodedVal)
			if err != nil {
				return err
			}
			if err := store.IndexStore.Put(ctx, tx, ent); err != nil {
				return ierrors.Wrap(err, "index put failed")
			}
			return nil
		},
	})
}
