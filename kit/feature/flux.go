package feature

import (
	"fmt"

	fluxfeature "github.com/influxdata/flux/dependencies/feature"
)

type fluxFlag struct {
	flag fluxfeature.Flag
}

func (f fluxFlag) Key() string {
	return f.flag.Key()
}

func (f fluxFlag) Default() interface{} {
	// Flux uses int for int flags and influxdb uses int32.
	// Convert to int32 here so influxdb understands our flag.
	switch v := f.flag.Default().(type) {
	case int:
		return int32(v)
	default:
		return v
	}
}

func (f fluxFlag) Expose() bool {
	return false
}

func (f fluxFlag) AuthenticationOptional() bool {
	return true
}

func init() {
	for _, flag := range fluxfeature.Flags() {
		if _, ok := byKey[flag.Key()]; ok {
			panic(fmt.Errorf("duplicate feature flag defined in flux and idpe: %s", flag.Key()))
		}
		wrappedFlag := fluxFlag{flag: flag}
		all = append(all, wrappedFlag)
		byKey[flag.Key()] = wrappedFlag
	}
}
