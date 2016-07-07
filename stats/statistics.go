package stats

import (
	"errors"
	"expvar"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// errUnexpectedRefCount is the panic used if there is an expected reference counting violation
var errUnexpectedRefCount = errors.New("unexpected reference counting error")

// The type which is used to implement both the Builder and Statistics interface
type statistics struct {
	// idleSince is the number of nanoseconds since the object has been busy
	// it is declared as the first member to guarantee required 64-bit alignment
	idleSince int64
	expvar.Map
	// mu guards access to mutable elements during the building phase
	mu sync.RWMutex
	// registry is a reference to a private interface of the Registry
	registry registryClient
	// key is a unique key for this object
	key string
	// name is the name of the measure into which these statistics will be written
	name string
	// tags are the tags that uniquely identify a distinct series in a measurement
	tags map[string]string
	// values is the Map into which statistic values are placed
	values *expvar.Map
	// intVars is go map which indexes the integer variables by name
	intVars map[string]*expvar.Int
	// stringVars is go map which indexes the string variables by name
	stringVars map[string]*expvar.String
	// floatVars is go map which indexes the float variables by name
	floatVars map[string]*expvar.Float
	// types records the declared type of each variable
	types map[string]string
	// busyCounters maps a variable name to the counter incremented when the variable is changed
	busyCounters map[string]*int32
	// built is false during building and true thereafter
	built bool
	// isRecorderOpen is true if the Recorder interface for the object is still open.
	isRecorderOpen bool
	// refsCount is the number of open references to this object
	refsCount int
	// busyCount is zero if the object is idle, and non-zero otherwise
	busyCount int32
	// notBusyCount is used to count updates to variables that don't affect the busy/idle state
	notBusyCount int32
	// disableIdleTimer is true if reports of these statistics are not be suppressed, even if they are idled
	disableIdleTimer bool
}

func (s *statistics) Key() string {
	return s.key
}

func (s *statistics) Name() string {
	return s.name
}

func (s *statistics) Tags() map[string]string {
	return s.tags
}

func (s *statistics) ValuesMap() *expvar.Map {
	return s.values
}

func (s *statistics) Values() map[string]interface{} {
	values := make(map[string]interface{})
	n := s.ValuesMap()
	n.Do(func(kv expvar.KeyValue) {
		var f interface{}
		var err error
		switch v := kv.Value.(type) {
		case *expvar.Float:
			f, err = strconv.ParseFloat(v.String(), 64)
		case *expvar.Int:
			f, err = strconv.ParseInt(v.String(), 10, 64)
		default:
			f, err = strconv.Unquote(v.String())
		}
		if err != nil {
			return
		}
		values[kv.Key] = f
	})
	return values
}

func (s *statistics) SetInt(n string, i int64) Recorder {
	s.assertDeclaredAs(n, "int")
	s.intVars[n].Set(i)
	atomic.AddInt32(s.busyCounters[n], 1)
	return s
}
func (s *statistics) SetFloat(n string, f float64) Recorder {
	s.assertDeclaredAs(n, "float")
	s.floatVars[n].Set(f)
	atomic.AddInt32(s.busyCounters[n], 1)
	return s
}

func (s *statistics) SetString(n string, v string) Recorder {
	s.assertDeclaredAs(n, "string")
	s.stringVars[n].Set(v)
	atomic.AddInt32(s.busyCounters[n], 1)
	return s
}

func (s *statistics) AddInt(n string, i int64) Recorder {
	s.assertDeclaredAs(n, "int")
	s.intVars[n].Add(i)
	atomic.AddInt32(s.busyCounters[n], 1)
	return s
}

func (s *statistics) AddFloat(n string, f float64) Recorder {
	s.assertDeclaredAs(n, "float")
	s.floatVars[n].Add(f)
	atomic.AddInt32(s.busyCounters[n], 1)
	return s
}

// Consideration should be given to either commenting out the implementation
// or the calls to this method. In well-tested code, it will never do
// anything useful. The main reason for leaving it in is to document
// the requirement that the Statistics methods should never be called
// with a name which was not previously declared.
//
// One option might be to leave this method in during transition to the new statistics
// API to provide helpful error messages to developers who might not have grok'd the
// requirements of the new API properly, and then remove it once the code
// base has been transitioned.
//
// This will have the advantage of communicating the requirements of the new API
// to developers without imposing a long term cost on the runtime.
func (s *statistics) assertDeclaredAs(n string, t string) {
	if declared, ok := s.types[n]; !ok || t != declared {
		if !ok {
			panic(ErrStatNotDeclared)
		} else {
			panic(ErrStatDeclaredWithDifferentType)
		}
	}
}

// Open the Recorder and register it with the registryClient
func (s *statistics) Open() Recorder {
	s.open(true)
	return s
}

// Close the Recorder.
func (s *statistics) Close() {
	s.close(true)
}

// Increment the reference count,
// set the isOpen() status and conditionally notify the
// registry of the new Recorder
func (s *statistics) open(owner bool) {
	s.mu.Lock()
	if owner {
		if s.isRecorderOpen {
			s.mu.Unlock()
			panic(ErrAlreadyOpen)
		}
		s.isRecorderOpen = true
	}
	s.refsCount++
	s.mu.Unlock()

	// Perform this notification outside of a lock.
	// Inside of a lock, there is no room to move.
	//
	// With apologies to Groucho Marx.
	if owner {
		s.registry.register(s)
	}
}

// Decrement the reference count and conditionally
// clear the isOpen().
func (s *statistics) close(owner bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.refsCount == 0 {
		panic(errUnexpectedRefCount)
	}

	if owner {
		if !s.isRecorderOpen {
			panic(ErrAlreadyClosed)
		}
		s.isRecorderOpen = false
	}
	s.refsCount--
	return s.refsCount
}

// True if the Recorder interface is still open.
func (s *statistics) isOpen() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRecorderOpen
}

func (s *statistics) IsOpen() bool {
	return s.isOpen()
}

// Return true if there is less than 2 references to the receiver
func (s *statistics) refs() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.refsCount
}

// Register an observer.
func (s *statistics) observe() {
	s.open(false)
}

// Deregister an observer.
func (s *statistics) stopObserving() int {
	return s.close(false)
}

// Update the idle time of the statistics
func (s *statistics) UpdateIdleTime() time.Duration {
	if s.disableIdleTimer {
		return time.Duration(0)
	}

	count := atomic.LoadInt32(&s.busyCount)
	atomic.StoreInt32(&s.busyCount, 0)

	now := time.Now().UnixNano()

	if count > 0 {
		atomic.StoreInt64(&s.idleSince, 0)
		return time.Duration(0)
	} else if then := atomic.LoadInt64(&s.idleSince); then == 0 {
		atomic.StoreInt64(&s.idleSince, now)
		return time.Duration(0)
	} else {
		return time.Duration(now - then)
	}
}
