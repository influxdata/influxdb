package stats

import (
	"expvar"
)

// Initializes a new Builder and associates with the specified registry.
func newBuilder(k string, n string, tags map[string]string, r registryClient) Builder {
	values := &expvar.Map{}
	values.Init()

	builder := &statistics{
		registry:     r,
		key:          k,
		name:         n,
		tags:         tags,
		values:       values,
		refsCount:    0,
		intVars:      map[string]*expvar.Int{},
		stringVars:   map[string]*expvar.String{},
		floatVars:    map[string]*expvar.Float{},
		types:        map[string]string{},
		busyCounters: map[string]*int32{},
	}

	builder.Init()

	s := &expvar.String{}
	s.Set(n)
	builder.Set("name", s)

	// values
	builder.Set("values", values)

	// tags and name
	tagsMap := &expvar.Map{}
	tagsMap.Init()
	for k, v := range tags {
		s := &expvar.String{}
		s.Set(v)
		tagsMap.Set(k, s)
	}
	builder.Set("tags", tagsMap)

	return builder
}

// Checks whether the receiver has already been built and returns an error if it has
func (s *statistics) checkNotBuilt() error {
	if s.built {
		return ErrAlreadyBuilt
	}
	return nil
}

// Calls checkNotBuilt and panic if an error is returned
func (s *statistics) assertNotBuilt() {
	if err := s.checkNotBuilt(); err != nil {
		panic(err)
	}
}

// Asserts that the specifed statistic has not already been declared.
func (s *statistics) assertNotDeclared(n string) {
	if _, ok := s.types[n]; ok {
		panic(ErrStatAlreadyDeclared)
	}
}

// Declare an integer statistic
func (s *statistics) DeclareInt(n string, iv int64) Builder {
	s.assertNotBuilt()
	s.assertNotDeclared(n)
	v := &expvar.Int{}
	v.Set(iv)
	s.values.Set(n, v)
	s.intVars[n] = v
	s.types[n] = "int"
	s.busyCounters[n] = &s.busyCount
	return s
}

// Declare a string statistic
func (s *statistics) DeclareString(n string, iv string) Builder {
	s.assertNotBuilt()
	s.assertNotDeclared(n)
	v := &expvar.String{}
	v.Set(iv)
	s.values.Set(n, v)
	s.stringVars[n] = v
	s.types[n] = "string"
	s.busyCounters[n] = &s.busyCount
	return s
}

// Declare a float statistic
func (s *statistics) DeclareFloat(n string, iv float64) Builder {
	s.assertNotBuilt()
	s.assertNotDeclared(n)
	v := &expvar.Float{}
	v.Set(iv)
	s.values.Set(n, v)
	s.floatVars[n] = v
	s.types[n] = "float"
	s.busyCounters[n] = &s.busyCount
	return s
}

func (s *statistics) DisableIdleTimer() Builder {
	s.disableIdleTimer = true
	return s
}

func (s *statistics) DontUpdateBusyCount(n string) Builder {
	s.busyCounters[n] = &s.notBusyCount
	return s
}

// Finish building a Statistics returning an error on failure
func (s *statistics) Build() (Recorder, error) {
	if err := s.checkNotBuilt(); err != nil {
		return nil, err
	}

	s.built = true
	tmp := &expvar.Map{}
	tmp.Init()
	s.values.Do(func(kv expvar.KeyValue) {
		tmp.Set(kv.Key, kv.Value)
	})
	s.values = tmp

	return s, nil
}

// Finish building a Statistics and panic on failure.
func (s *statistics) MustBuild() Recorder {
	if set, err := s.Build(); err != nil {
		panic(err)
	} else {
		return set
	}
}
