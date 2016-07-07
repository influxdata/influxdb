package stats

// A type used views to register observers of new registrations
// and to issuing clean hints.
type registryClient interface {
	// Called by a view object to register a listener for new registrations.
	onOpen(lf func(o registration)) func()
	// Called by a statistics object to register itself upon Open.
	register(r registration)
	// Called by the client when it detects that cleaning is required.
	clean()
}

// This type is used by the View and the Registry to manage the
// life cycle and visibility of statistics within the registry
// and the view.
type registration interface {
	Statistics
	// True if the recorder has not yet closed this object.
	isOpen() bool
	// Increment the number observers
	observe()
	// Decrement the number of observers.
	stopObserving() int
	// The number of open references to the receiver.
	refs() int
}
