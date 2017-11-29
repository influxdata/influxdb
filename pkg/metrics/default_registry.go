package metrics

var defaultRegistry = NewRegistry()

// MustRegisterGroup registers a new group using the specified name.
// If the group name is not unique, MustRegisterGroup will panic.
//
// MustRegisterGroup is not safe to call from multiple goroutines.
func MustRegisterGroup(name string) GID {
	return defaultRegistry.MustRegisterGroup(name)
}

// MustRegisterCounter registers a new counter metric with the default registry
// using the provided descriptor.
// If the metric name is not unique, MustRegisterCounter will panic.
//
// MustRegisterCounter is not safe to call from multiple goroutines.
func MustRegisterCounter(name string, opts ...descOption) ID {
	return defaultRegistry.MustRegisterCounter(name, opts...)
}

// MustRegisterTimer registers a new timer metric with the default registry
// using the provided descriptor.
// If the metric name is not unique, MustRegisterTimer will panic.
//
// MustRegisterTimer is not safe to call from multiple goroutines.
func MustRegisterTimer(name string, opts ...descOption) ID {
	return defaultRegistry.MustRegisterTimer(name, opts...)
}

// NewGroup returns a new measurement group from the default registry.
//
// NewGroup is safe to call from multiple goroutines.
func NewGroup(gid GID) *Group {
	return defaultRegistry.NewGroup(gid)
}
