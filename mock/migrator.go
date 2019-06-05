package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

// MigratorService is a mocked migration service.
type MigratorService struct {
	Data map[string]map[string][]byte
}

// GetAll returns all the data within that bucket.
func (s *MigratorService) GetAll(bucket []byte) map[string][]byte {
	if s.Data == nil {
		s.Data = make(map[string]map[string][]byte)
	}
	if s.Data[string(bucket)] == nil {
		s.Data[string(bucket)] = make(map[string][]byte)
	}
	return s.Data[string(bucket)]
}

// DryRun will do a scan of the current data, and stream the error to the channel.
func (s *MigratorService) DryRun(ctx context.Context, m influxdb.Migrator, up bool, errCh chan<- error) {
	// do nothing
}

// Migrate will do the migration and store the data.
func (s *MigratorService) Migrate(ctx context.Context, m influxdb.Migrator, up bool, limit int) error {
	bucket := m.Bucket()
	data := s.GetAll(bucket)
	var dst []byte
	var err error
	for key, src := range data {
		if up {
			dst, err = m.Up(src)
		} else {
			dst, err = m.Down(src)
		}
		if err != nil {
			return err
		}
		s.Data[string(bucket)][string(key)] = dst
	}
	return nil
}
