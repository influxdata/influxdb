package dbrp

// The DBRP Mapping `Service` maps database, retention policy pairs to buckets.
// Every `DBRPMapping` stored is scoped to an organization ID.
// The service must ensure the following invariants are valid at any time:
//  - each orgID, database, retention policy triple must be unique;
//  - for each orgID and database there must exist one and only one default mapping (`mapping.Default` set to `true`).
// The service does so using three kv buckets:
//  - one for storing mappings;
//  - one for storing an index of mappings by orgID and database;
//  - one for storing the current default mapping for an orgID and a database.
//
// On *create*, the service creates the mapping.
// If another mapping with the same orgID, database, and retention policy exists, it fails.
// If the mapping is the first one for the specified orgID-database couple, it will be the default one.
//
// On *find*, the service find mappings.
// Every mapping returned uses the kv bucket where the default is specified to update the `mapping.Default` field.
//
// On *update*, the service updates the mapping.
// If the update causes another bucket to have the same orgID, database, and retention policy, it fails.
// If the update unsets `mapping.Default`, the first mapping found is set as default.
//
// On *delete*, the service updates the mapping.
// If the deletion deletes the default mapping, the first mapping found is set as default.

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	bucket             = []byte("dbrpv1")
	indexBucket        = []byte("dbrpbyorganddbindexv1")
	byOrgIDIndexBucket = []byte("dbrpbyorgv1")
	defaultBucket      = []byte("dbrpdefaultv1")
)

var _ influxdb.DBRPMappingServiceV2 = (*AuthorizedService)(nil)

type Service struct {
	store kv.Store
	IDGen platform.IDGenerator

	bucketSvc        influxdb.BucketService
	byOrgAndDatabase *kv.Index
	byOrg            *kv.Index
}

func indexForeignKey(dbrp influxdb.DBRPMappingV2) []byte {
	return composeForeignKey(dbrp.OrganizationID, dbrp.Database)
}

func composeForeignKey(orgID platform.ID, db string) []byte {
	encID, _ := orgID.Encode()
	key := make([]byte, len(encID)+len(db))
	copy(key, encID)
	copy(key[len(encID):], db)
	return key
}

func NewService(ctx context.Context, bucketSvc influxdb.BucketService, st kv.Store) influxdb.DBRPMappingServiceV2 {
	return &Service{
		store:     st,
		IDGen:     snowflake.NewDefaultIDGenerator(),
		bucketSvc: bucketSvc,
		byOrgAndDatabase: kv.NewIndex(kv.NewIndexMapping(bucket, indexBucket, func(v []byte) ([]byte, error) {
			var dbrp influxdb.DBRPMappingV2
			if err := json.Unmarshal(v, &dbrp); err != nil {
				return nil, err
			}
			return indexForeignKey(dbrp), nil
		}), kv.WithIndexReadPathEnabled),
		byOrg: kv.NewIndex(ByOrgIDIndexMapping, kv.WithIndexReadPathEnabled),
	}
}

// getDefault gets the default mapping ID inside of a transaction.
func (s *Service) getDefault(tx kv.Tx, compKey []byte) ([]byte, error) {
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return nil, err
	}
	defID, err := b.Get(compKey)
	if err != nil {
		return nil, err
	}
	return defID, nil
}

// getDefaultID returns the default mapping ID for the given orgID and db.
func (s *Service) getDefaultID(tx kv.Tx, compKey []byte) (platform.ID, error) {
	defID, err := s.getDefault(tx, compKey)
	if err != nil {
		return 0, err
	}
	id := new(platform.ID)
	if err := id.Decode(defID); err != nil {
		return 0, err
	}
	return *id, nil
}

// isDefault tells whether a mapping is the default one.
func (s *Service) isDefault(tx kv.Tx, compKey []byte, id []byte) (bool, error) {
	defID, err := s.getDefault(tx, compKey)
	if kv.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return bytes.Equal(id, defID), nil
}

// isDefaultSet tells if there is a default mapping for the given composite key.
func (s *Service) isDefaultSet(tx kv.Tx, compKey []byte) (bool, error) {
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return false, ErrInternalService(err)
	}
	_, err = b.Get(compKey)
	if kv.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, ErrInternalService(err)
	}
	return true, nil
}

// setAsDefault sets the given id as default for the given composite key.
func (s *Service) setAsDefault(tx kv.Tx, compKey []byte, id []byte) error {
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return ErrInternalService(err)
	}
	if err := b.Put(compKey, id); err != nil {
		return ErrInternalService(err)
	}
	return nil
}

// unsetDefault un-sets the default for the given composite key.
// Useful when a db/rp pair does not exist anymore.
func (s *Service) unsetDefault(tx kv.Tx, compKey []byte) error {
	b, err := tx.Bucket(defaultBucket)
	if err != nil {
		return ErrInternalService(err)
	}
	if err = b.Delete(compKey); err != nil {
		return ErrInternalService(err)
	}
	return nil
}

// getFirstBut returns the first element in the db/rp index (not accounting for the `skipID`).
// If the length of the returned ID is 0, it means no element was found.
// The skip value is useful, for instance, if one wants to delete an element based on the result of this operation.
func (s *Service) getFirstBut(tx kv.Tx, compKey []byte, skipID []byte) (next []byte, err error) {
	err = s.byOrgAndDatabase.Walk(context.Background(), tx, compKey, func(k, v []byte) (bool, error) {
		if bytes.Equal(skipID, k) {
			return true, nil
		}

		next = k

		return false, nil
	})
	return
}

// isDBRPUnique verifies if the triple orgID-database-retention-policy is unique.
func (s *Service) isDBRPUnique(ctx context.Context, m influxdb.DBRPMappingV2) error {
	return s.store.View(ctx, func(tx kv.Tx) error {
		return s.byOrgAndDatabase.Walk(ctx, tx, composeForeignKey(m.OrganizationID, m.Database), func(k, v []byte) (bool, error) {
			dbrp := &influxdb.DBRPMappingV2{}
			if err := json.Unmarshal(v, dbrp); err != nil {
				return false, ErrInternalService(err)
			}

			if dbrp.ID == m.ID {
				// Corner case.
				// This is the very same DBRP, just skip it!
				return true, nil
			}

			if dbrp.RetentionPolicy == m.RetentionPolicy {
				return false, ErrDBRPAlreadyExists("another DBRP mapping with same orgID, db, and rp exists")
			}

			return true, nil
		})
	})
}

// FindBy returns the mapping for the given ID.
func (s *Service) FindByID(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMappingV2, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidDBRPID(id.String(), err)
	}

	m := &influxdb.DBRPMappingV2{}
	if err := s.store.View(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		b, err := bucket.Get(encodedID)
		if err != nil {
			return ErrDBRPNotFound
		}
		if err := json.Unmarshal(b, m); err != nil {
			return ErrInternalService(err)
		}
		// If the given orgID is wrong, it is as if we did not found a mapping scoped to this org.
		if m.OrganizationID != orgID {
			return ErrDBRPNotFound
		}
		// Update the default value for this mapping.
		m.Default, err = s.isDefault(tx, indexForeignKey(*m), encodedID)
		if err != nil {
			return ErrInternalService(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return m, nil
}

// FindMany returns a list of mappings that match filter and the total count of matching dbrp mappings.
// TODO(affo): find a smart way to apply FindOptions to a list of items.
func (s *Service) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	// Memoize default IDs.
	defs := make(map[string]*platform.ID)
	get := func(tx kv.Tx, orgID platform.ID, db string) (*platform.ID, error) {
		k := orgID.String() + db
		if _, ok := defs[k]; !ok {
			id, err := s.getDefaultID(tx, composeForeignKey(orgID, db))
			if kv.IsNotFound(err) {
				// Still need to store a not-found result.
				defs[k] = nil
			} else if err != nil {
				return nil, err
			} else {
				defs[k] = &id
			}
		}
		return defs[k], nil
	}

	ms := []*influxdb.DBRPMappingV2{}
	add := func(tx kv.Tx) func(k, v []byte) (bool, error) {
		return func(k, v []byte) (bool, error) {
			m := influxdb.DBRPMappingV2{}
			if err := json.Unmarshal(v, &m); err != nil {
				return false, ErrInternalService(err)
			}
			// Updating the Default field must be done before filtering.
			defID, err := get(tx, m.OrganizationID, m.Database)
			if err != nil {
				return false, ErrInternalService(err)
			}

			m.Default = m.ID == *defID
			if filterFunc(&m, filter) {
				ms = append(ms, &m)
			}
			return true, nil
		}
	}

	err := s.store.View(ctx, func(tx kv.Tx) error {
		// Optimized path, use index.
		if orgID := filter.OrgID; orgID != nil {
			var (
				db      = ""
				compKey []byte
				index   *kv.Index
			)
			if filter.Database != nil && len(*filter.Database) > 0 {
				db = *filter.Database
				compKey = composeForeignKey(*orgID, db)
				index = s.byOrgAndDatabase

				// Filtering by Org, Database and Default == true
				if def := filter.Default; def != nil && *def {
					defID, err := s.getDefault(tx, compKey)
					if kv.IsNotFound(err) {
						return nil
					}
					if err != nil {
						return ErrInternalService(err)
					}
					bucket, err := tx.Bucket(bucket)
					if err != nil {
						return ErrInternalService(err)
					}
					v, err := bucket.Get(defID)
					if err != nil {
						return ErrInternalService(err)
					}
					_, err = add(tx)(defID, v)
					return err
				}
			} else {
				compKey, _ = orgID.Encode()
				index = s.byOrg
			}

			return index.Walk(ctx, tx, compKey, add(tx))
		}
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		cur, err := bucket.Cursor()
		if err != nil {
			return ErrInternalService(err)
		}

		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if _, err := add(tx)(k, v); err != nil {
				return err
			}
		}
		return nil
	})

	return ms, len(ms), err
}

// Create creates a new mapping.
// If another mapping with same organization ID, database, and retention policy exists, an error is returned.
// If the mapping already contains a valid ID, that one is used for storing the mapping.
func (s *Service) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if !dbrp.ID.Valid() {
		dbrp.ID = s.IDGen.ID()
	}
	if err := dbrp.Validate(); err != nil {
		return ErrInvalidDBRP(err)
	}

	if _, err := s.bucketSvc.FindBucketByID(ctx, dbrp.BucketID); err != nil {
		return err
	}

	// If a dbrp with this particular ID already exists an error is returned.
	if _, err := s.FindByID(ctx, dbrp.OrganizationID, dbrp.ID); err == nil {
		return ErrDBRPAlreadyExists("dbrp already exist for this particular ID. If you are trying an update use the right function .Update")
	}
	// If a dbrp with this orgID, db, and rp exists an error is returned.
	if err := s.isDBRPUnique(ctx, *dbrp); err != nil {
		return err
	}

	encodedID, err := dbrp.ID.Encode()
	if err != nil {
		return ErrInvalidDBRPID(dbrp.ID.String(), err)
	}

	// OrganizationID has been validated by Validate
	orgID, _ := dbrp.OrganizationID.Encode()

	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}

		// populate indices
		compKey := indexForeignKey(*dbrp)
		if err := s.byOrgAndDatabase.Insert(tx, compKey, encodedID); err != nil {
			return err
		}

		if err := s.byOrg.Insert(tx, orgID, encodedID); err != nil {
			return err
		}

		defSet, err := s.isDefaultSet(tx, compKey)
		if err != nil {
			return err
		}
		if !defSet {
			dbrp.Default = true
		}

		b, err := json.Marshal(dbrp)
		if err != nil {
			return ErrInternalService(err)
		}
		if err := bucket.Put(encodedID, b); err != nil {
			return ErrInternalService(err)
		}

		if dbrp.Default {
			if err := s.setAsDefault(tx, compKey, encodedID); err != nil {
				return err
			}
		}
		return nil
	})
}

// Updates a mapping.
// If another mapping with same organization ID, database, and retention policy exists, an error is returned.
// Un-setting `Default` for a mapping will cause the first one to become the default.
func (s *Service) Update(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if err := dbrp.Validate(); err != nil {
		return ErrInvalidDBRP(err)
	}
	oldDBRP, err := s.FindByID(ctx, dbrp.OrganizationID, dbrp.ID)
	if err != nil {
		return ErrDBRPNotFound
	}
	// Overwrite fields that cannot change.
	dbrp.ID = oldDBRP.ID
	dbrp.OrganizationID = oldDBRP.OrganizationID
	dbrp.BucketID = oldDBRP.BucketID
	dbrp.Database = oldDBRP.Database

	// If a dbrp with this orgID, db, and rp exists an error is returned.
	if err := s.isDBRPUnique(ctx, *dbrp); err != nil {
		return err
	}

	encodedID, err := dbrp.ID.Encode()
	if err != nil {
		return ErrInternalService(err)
	}
	b, err := json.Marshal(dbrp)
	if err != nil {
		return ErrInternalService(err)
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		if err := bucket.Put(encodedID, b); err != nil {
			return err
		}
		compKey := indexForeignKey(*dbrp)
		if dbrp.Default {
			err = s.setAsDefault(tx, compKey, encodedID)
		} else if oldDBRP.Default {
			// This means default was unset.
			// Need to find a new default.
			first, ferr := s.getFirstBut(tx, compKey, encodedID)
			if ferr != nil {
				return ferr
			}
			if len(first) > 0 {
				err = s.setAsDefault(tx, compKey, first)
			}
			// If no first was found, then this will remain the default.
		}
		return err
	})
}

// Delete removes a mapping.
// Deleting a mapping that does not exists is not an error.
// Deleting the default mapping will cause the first one (if any) to become the default.
func (s *Service) Delete(ctx context.Context, orgID, id platform.ID) error {
	dbrp, err := s.FindByID(ctx, orgID, id)
	if err != nil {
		return nil
	}
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInternalService(err)
	}

	encodedOrgID, err := orgID.Encode()
	if err != nil {
		return ErrInternalService(err)
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := tx.Bucket(bucket)
		if err != nil {
			return ErrInternalService(err)
		}
		compKey := indexForeignKey(*dbrp)
		if err := bucket.Delete(encodedID); err != nil {
			return err
		}
		if err := s.byOrgAndDatabase.Delete(tx, compKey, encodedID); err != nil {
			return ErrInternalService(err)
		}
		if err := s.byOrg.Delete(tx, encodedOrgID, encodedID); err != nil {
			return ErrInternalService(err)
		}
		// If this was the default, we need to set a new default.
		var derr error
		if dbrp.Default {
			first, err := s.getFirstBut(tx, compKey, encodedID)
			if err != nil {
				return err
			}
			if len(first) > 0 {
				derr = s.setAsDefault(tx, compKey, first)
			} else {
				// This means no other mapping is in the index.
				// Unset the default
				derr = s.unsetDefault(tx, compKey)
			}
		}
		return derr
	})
}

// filterFunc is capable to validate if the dbrp is valid from a given filter.
// it runs true if the filtering data are contained in the dbrp.
func filterFunc(dbrp *influxdb.DBRPMappingV2, filter influxdb.DBRPMappingFilterV2) bool {
	return (filter.ID == nil || (*filter.ID) == dbrp.ID) &&
		(filter.OrgID == nil || (*filter.OrgID) == dbrp.OrganizationID) &&
		(filter.BucketID == nil || (*filter.BucketID) == dbrp.BucketID) &&
		(filter.Database == nil || (*filter.Database) == dbrp.Database) &&
		(filter.RetentionPolicy == nil || (*filter.RetentionPolicy) == dbrp.RetentionPolicy) &&
		(filter.Default == nil || (*filter.Default) == dbrp.Default)
}
