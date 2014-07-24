package storage

import (
	"fmt"
	"strings"
)

var engineRegistry = make(map[string]Initializer)

type Initializer struct {
	NewConfig  func() interface{}
	Initialize func(path string, config interface{}) (Engine, error)
}

func wrapInitializer(initializer Initializer) Initializer {
	var init Initializer
	init.Initialize = func(path string, config interface{}) (Engine, error) {
		engine, err := initializer.Initialize(path, config)
		if err != nil {
			return nil, err
		}

		// these sentinel values are here so that we can seek to the end of
		// the keyspace and have the iterator still be valid.  this is for
		// the series that is at either end of the keyspace.
		engine.Put([]byte(strings.Repeat("\x00", 24)), []byte{})
		engine.Put([]byte(strings.Repeat("\xff", 24)), []byte{})
		return engine, nil
	}
	init.NewConfig = initializer.NewConfig
	return init
}

func registerEngine(name string, init Initializer) {
	if _, ok := engineRegistry[name]; ok {
		panic(fmt.Errorf("Engine '%s' already exists", name))
	}
	engineRegistry[name] = wrapInitializer(init)
}

func GetInitializer(name string) (Initializer, error) {
	initializer, ok := engineRegistry[name]
	if !ok {
		return initializer, fmt.Errorf("Engine '%s' not found", name)
	}

	return initializer, nil
}
