package pprofutil

import (
	"os"
	"runtime/pprof"
)

type Profile struct {
	*pprof.Profile

	Path  string
	Debug int
}

func NewProfile(name, path string, debug int) *Profile {
	p := &Profile{Profile: pprof.NewProfile(name), Path: path, Debug: debug}
	return p
}

func (p *Profile) Stop() {
	f, err := os.Create(p.Path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := p.WriteTo(f, p.Debug); err != nil {
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}

	println("pprof profile written:", p.Path)
}
