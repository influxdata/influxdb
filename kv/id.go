package kv

import (
	"context"
	"errors"
	"math/rand"

	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

var (
	idsBucket     = []byte("idsv1")
	idKey         = []byte("id")
	errIDNotFound = errors.New("id not found")
)

var _ influxdb.IDGenerator = (*Service)(nil)

func (s *Service) initializeID(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(idsBucket); err != nil {
		return err
	}

	return nil
}

// ID retrieves the unique ID for this influx instance.
func (s *Service) ID() influxdb.ID {
	// if any error occurs return a random number
	id := influxdb.ID(rand.Int63())
	err := s.kv.Update(context.Background(), func(tx Tx) error {
		val, err := s.getID(tx)
		if err != nil && err != errIDNotFound {
			return err
		}

		if err == errIDNotFound {
			if val, err = s.generateID(tx); err != nil {
				s.Logger.Error("unable to generate id", zap.Error(err))
				return err
			}
		}

		id = val
		return nil
	})

	if err != nil {
		s.Logger.Error("unable to load id", zap.Error(err))
	}

	return id
}

func (s *Service) getID(tx Tx) (influxdb.ID, error) {
	b, err := tx.Bucket(idsBucket)
	if err != nil {
		return influxdb.InvalidID(), &influxdb.Error{
			Err: err,
		}
	}

	v, err := b.Get(idKey)
	if err != nil && !IsNotFound(err) {
		return influxdb.InvalidID(), err
	}

	if IsNotFound(err) {
		return influxdb.InvalidID(), errIDNotFound
	}

	var id influxdb.ID
	if err := id.Decode(v); err != nil {
		return influxdb.InvalidID(), err
	}
	return id, nil
}

func (s *Service) generateID(tx Tx) (influxdb.ID, error) {
	id := s.IDGenerator.ID()
	encodedID, err := id.Encode()
	if err != nil {
		return influxdb.InvalidID(), err
	}

	b, err := tx.Bucket(idsBucket)
	if err != nil {
		return influxdb.InvalidID(), err
	}

	if err := b.Put(idKey, encodedID); err != nil {
		return influxdb.InvalidID(), err
	}

	return id, nil
}
