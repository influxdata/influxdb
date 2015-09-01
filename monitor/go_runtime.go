package monitor

import (
	"expvar"
)

type goRuntime struct{}

func (g *goRuntime) Statistics() (*expvar.Map, error) {
	m := expvar.Get("memstats").(*expvar.Map)
	return m, nil
}

func (g *goRuntime) Diagnostics() (map[string]interface{}, error) {
	return nil, nil
}
