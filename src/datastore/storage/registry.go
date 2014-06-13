package storage

import "fmt"

var engineRegistry = make(map[string]Initializer)

type Initializer struct {
	NewConfig  func() interface{}
	Initialize func(path string, config interface{}) (Engine, error)
}

func registerEngine(name string, init Initializer) {
	if _, ok := engineRegistry[name]; ok {
		panic(fmt.Errorf("Engine '%s' already exists", name))
	}
	engineRegistry[name] = init
}

func GetInitializer(name string) (Initializer, error) {
	initializer, ok := engineRegistry[name]
	if !ok {
		return initializer, fmt.Errorf("Engine '%s' not found", name)
	}

	return initializer, nil
}
