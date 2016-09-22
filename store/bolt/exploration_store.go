package bolt

import (
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/store/bolt/internal"
	"golang.org/x/net/context"
)

// Ensure ExplorationStore implements mrfusion.ExplorationStore.
var _ mrfusion.ExplorationStore = &ExplorationStore{}

type ExplorationStore struct {
	session *Session
}

// Search the ExplorationStore for all explorations owned by userID.
func (s *ExplorationStore) Query(ctx context.Context, uid mrfusion.UserID) ([]mrfusion.Exploration, error) {
	// Begin read transaction.
	tx, err := s.session.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var explorations []mrfusion.Exploration
	if err := tx.Bucket([]byte("Explorations")).ForEach(func(k, v []byte) error {
		var e mrfusion.Exploration
		if err := internal.UnmarshalExploration(v, &e); err != nil {
			return err
		} else if e.UserID != uid {
			return nil
		}
		explorations = append(explorations, e)
		return nil
	}); err != nil {
		return nil, err
	}

	return explorations, nil
}

// Create a new Exploration in the ExplorationStore.
func (s *ExplorationStore) Add(ctx context.Context, e mrfusion.Exploration) error {
	// Begin read-write transaction.
	tx, err := s.session.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte("Explorations"))
	seq, _ := b.NextSequence()
	e.ID = mrfusion.ExplorationID(seq)
	e.CreatedAt = s.session.now

	if v, err := internal.MarshalExploration(&e); err != nil {
		return err
	} else if err := b.Put(itob(int(e.ID)), v); err != nil {
		return err
	}

	return tx.Commit()
}

// Delete the exploration from the ExplorationStore
func (s *ExplorationStore) Delete(ctx context.Context, e mrfusion.Exploration) error {
	// Begin read transaction.
	tx, err := s.session.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.Bucket([]byte("Explorations")).Delete(itob(int(e.ID))); err != nil {
		return err
	}

	return nil
}

// Retrieve an exploration for an id exists.
func (s *ExplorationStore) Get(ctx context.Context, id mrfusion.ExplorationID) (mrfusion.Exploration, error) {
	var e mrfusion.Exploration

	// Begin read transaction.
	tx, err := s.session.db.Begin(false)
	if err != nil {
		return e, err
	}
	defer tx.Rollback()

	if v := tx.Bucket([]byte("Explorations")).Get(itob(int(id))); v == nil {
		return e, nil
	} else if err := internal.UnmarshalExploration(v, &e); err != nil {
		return e, err
	}

	return e, nil
}

// Update the exploration with the exploration `ne`; will update `UpdatedAt`.
func (s *ExplorationStore) Update(ctx context.Context, ne mrfusion.Exploration) error {
	// Begin read-write transaction.
	tx, err := s.session.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte("Explorations"))

	var e mrfusion.Exploration
	if v := b.Get(itob(int(ne.ID))); v == nil {
		return ErrExplorationNotFound
	} else if err := internal.UnmarshalExploration(v, &e); err != nil {
		return err
	}

	e.Name = ne.Name
	e.UserID = ne.UserID
	e.Data = ne.Data
	e.UpdatedAt = s.session.now

	if v, err := internal.MarshalExploration(&e); err != nil {
		return err
	} else if err := b.Put(itob(int(e.ID)), v); err != nil {
		return err
	}

	return tx.Commit()
}
