package cluster

import (
	"log"
	"os"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

// MetaWriter writes meta query changes to the local tsdb store.
type MetaWriter struct {
	TSDBStore *tsdb.Store
	Logger    *log.Logger

	MetaClient interface {
		Database(name string) (*meta.DatabaseInfo, error)
	}
}

// NewMetaWriter returns a new initialized *MetaWriter.
func NewMetaWriter() *MetaWriter {
	return &MetaWriter{
		Logger: log.New(os.Stderr, "[meta-writer] ", log.LstdFlags),
	}
}

// DropDatabase closes and deletes all local files for the database.
func (m *MetaWriter) DropDatabase(name string) error {
	println("MetaWriter.DropDatabase start")
	defer println("MetaWriter.DropDatabase end")
	dbi, err := m.MetaClient.Database(name)
	if err != nil {
		return err
	} else if dbi == nil {
		return nil
	}

	// Remove the database from the local store
	return m.TSDBStore.DeleteDatabase(name)
}
