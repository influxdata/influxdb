package storage

import "fmt"

type Init func(path string) (Engine, error)

var engineRegistry = make(map[string]Init)

func registerEngine(name string, initializer Init) {
	if _, ok := engineRegistry[name]; ok {
		panic(fmt.Errorf("Engine '%s' already exists", name))
	}
	engineRegistry[name] = initializer
}

func GetEngine(name, path string) (Engine, error) {
	initializer := engineRegistry[name]
	if initializer == nil {
		return nil, fmt.Errorf("Engine '%s' not found", name)
	}

	return initializer(path)
}
