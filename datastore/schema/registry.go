package schema

import (
	"fmt"

	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/metastore"
)

var schemaRegistry = map[string]Initializer{}

// Convenience struct for holding initialization functions
type initializer struct {
	newConfigFunc  func() interface{}
	initializeFunc func(db storage.Engine, metaStore *metastore.Store, pointBatchSize, writeBatchSize int, config interface{}) (Schema, error)
}

// initializer implements the Initializer interface
func (self initializer) NewConfig() interface{} {
	return self.newConfigFunc()
}

// initializer implements the Initializer interface
func (self initializer) Initialize(db storage.Engine, metaStore *metastore.Store, pointBatchSize, writeBatchSize int, config interface{}) (Schema, error) {
	return self.initializeFunc(db, metaStore, pointBatchSize, writeBatchSize, config)
}

type Initializer interface {
	NewConfig() interface{}
	Initialize(db storage.Engine, metaStore *metastore.Store, pointBatchSize, writeBatchSize int, config interface{}) (Schema, error)
}

func registerSchema(name string, init Initializer) {
	if _, ok := schemaRegistry[name]; ok {
		panic(fmt.Errorf("schema: Schema '%s' already exists", name))
	}
	schemaRegistry[name] = init
}

func GetInitializer(name string) (Initializer, error) {
	init, ok := schemaRegistry[name]
	if !ok {
		return init, fmt.Errorf("schema: Schema '%s' not found", name)
	}
	return init, nil
}
