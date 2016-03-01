// Package stats defines a statistics acquisition and monitoring API
// which uses the go 'expvar' API as the underlying representation mechanism.
//
// The main difference between this API and the raw expvar API are the following:
//
// - this API does not leak memory if an unbounded number of short-lived Statistics objects are
//   created. Naive use of the expvar Map API for the same purpose is not well-suited
//   to this task because that API does not have a Delete method. There are ways to
//   workaround the issue, but they are somewhat complex and kludgy.
// - since Recorder Add/Set methods write directly into pre-allocated expvar Vars referenced
//   by go maps that never change, minimal locking is required in the recording path.
// - the Statistics objects created by this API are not directly reachable with a walk of the
//   expvar tree. Instead, a 'walk' function is provided which permits iteration over
//   the current set of statistics.
//
// In addition a View mechanism permits a background thread that is periodically polling
// the Registry of Statistics to see the last update made to a set of Statistics since
// the last polling cycle, even if the recording thread has closed the associated
// Recorder in the intervening time. This is achieved with a reference counting mechanism
// which ensures that a Statistics objects is not removed from the View until
// the consumer of the View has "seen" the Close event.
//
// The Recorder interface is a facade that manages the construction of expvar Map objects and
// their members - once the Recorder is initialised, the programmer has a single interface to
// interact which is simpler to understand than the several different types that one has
// to juggle with the raw expvar API. For example:
//
//    recorder.SetInt("level", int64(1))
//
// is easier to code and read, than:
//
//    r := expvar.Int{}
//    r.Set(int64(1))
//    m.Set("level", r)
//
// The Recorder interface exposes Set and Add methods that allow lock-free access
// to slices of the underlying expvar Map variables and a Close() method that can be used
// by the described object to release resources associated with the Recorder and remove
// the Statistics object from any open Views.
//
// In code:
//
//     foo.stats := stats.Root.
//        NewBuilder("cpu:1", "cpu", map[string]string{"tag": "T"}). // stats.Builder
//        DeclareInt("counter",0).                          // stats.Builder
//        MustBuild()                                      // stats.Recorder
//
//
// When the described object is opened:
//
//     foo.Stats.Open()
//
// During the lifetime of the foo object:
//
//     foo.stats.AddInt("counter", 1)
//
// Some time later, when foo is closed:
//
//     foo.stats.Close()
//
// The API is designed so that consumers of Statistics objects get exactly
// the interface they need to perform the task their role requires and no more.
//
// For example, consider the life cycle of a Statistics object.
//
// A Statistics object starts out as a Builder, obtained from stats.Root.NewBuilder(),
// by the object it describes - the so-called 'described object'. The described object then configures
// the Builder by declaring which statistics will be written into the Recorder when the
// Recorder becomes live. Once the declarations are finished, the described object calls
// MustBuild() to obtain a reference to the Recorder interface. Assuming this
// call succeeds, the caller then invokes the Recorder.Open() method to
// register the Recorder with the Registry. The Recorder interface is only used by 'described objects';
// observers of the statistics Registry only see instances of Statistics, an interface which does not
// include methods that allow the mutation or closing of the underlying Recorder object.
//
package stats

import (
	"errors"
	"expvar"
	"time"
)

// Builder is an interface used by described objects to declare the statistics
// they will generate during their life cycle. Any statistic name which is
// not declared to the Builder will cause the resulting to Recorder to panic
// if it is ever passed to the Add*() or Set*() methods.
//
// Any given name may be used as the parameter of a Declare*() method at most once.
//
// It is an error to call the Declare*() methods after Build() or MustBuild() have been called.
//
// The Build() and MustBuild() methods of Builder may be used at most once.
//
// To obtain a Builder instance, call stats.Root.NewBuilder().
type Builder interface {

	// The Declare methods are used to declare which statistics the resulting
	// Recorder will write.

	// Declare an integer statistic.
	DeclareInt(n string, iv int64) Builder
	// Declare an string statistic
	DeclareString(n string, iv string) Builder
	// Declare an float statistic
	DeclareFloat(n string, iv float64) Builder
	// DisbleIdleTimer disables the idle timer for these statistics
	DisableIdleTimer() Builder
	// Don't update the busy counter even if this stat changes.
	DontUpdateBusyCount(n string) Builder
	// Build a Recorder instance or return an error.
	Build() (Recorder, error)
	// Build a Recorder instance or panic.
	MustBuild() Recorder
}

// Statistics is the type that provides a read-only view of the statistics recorded by a Recorder.
//
// Observers of Statistics obtain access to Statistics objects by first opening a View() and then
// passing an iterator function to the View.Do() method.
type Statistics interface {
	expvar.Var

	// The statistic key.
	Key() string

	// The name of the statistic set.
	Name() string

	// The statistic tags.
	Tags() map[string]string

	// A map of the statistics using their corrsponding native go types.
	Values() map[string]interface{}

	// The underlying values map - do not use directly, provided for legacy expvar support only.
	ValuesMap() *expvar.Map

	// Update the idle time of the Statistics object, returning the idle duration.
	UpdateIdleTime() time.Duration
}

// The Recorder type is used by described objects to update statistics either by
// setting new values (for level-type statistics) or adding to an existing value (counter-type)
// statistics. Only statistic names which were declared during the construction of the Recorder
// may be used with a Recorder's Set and Add methods.
//
// At the end of the described object's life cycle, the described object should call Recorder.Close() method.
// This will release the resources associated with the Recorder and prevent any observers from publishing
// additional updates for that Recorder.
//
// Remember to close the Recorder instance at the end of the described object's life cycle by calling
// the Recorder.Close() method.
//
// To obtain a Recorder, call the MustBuild() method of a Builder. For example:
//
//     var recorder stats.Recorder = stats.Root.
//        NewBuilder("key", map[string]string{"tag": "T"}).
//        DeclareInt("counter",0).
//        MustBuild()
//
type Recorder interface {
	Statistics
	// Open the Recorder to allow views to see it.
	Open() Recorder
	// True if the Recorder is (still) open.
	IsOpen() bool
	// Set a level statistics to a particular integer value
	SetInt(n string, i int64) Recorder
	// Set a level statistic to a particular float value
	SetFloat(n string, f float64) Recorder
	// Set a string statistic
	SetString(n string, s string) Recorder

	// Add an int value to an int statistic
	AddInt(n string, i int64) Recorder
	// Add a float value to a float statistic
	AddFloat(n string, f float64) Recorder
	// Drop one reference to the object obtained with Open(). T
	// The described object closes the Statistics object first, then the monitor.
	Close()
}

// The Registry type allows described objects to obtain Builders used
// to construct their Recorder instances. For example: a described object might be
// implemented like this:
//
//    import "github.com/influxdata/influxdb/stats"
//
//    type FooBar struct {
//        stats stats.Recorder
//    }
//
//    ...
//
//    fooBar := &FooBar{
//        stats: stats.Root.NewBuilder("foobar:1", "foobar", map[string]string{"tags": "T"}).
//           DeclareInt("counter", 0).
//           MustBuild().
//           Open(),
//    }
//
//    ...
//
//    func (fb * FooBar) Close() {
//        fb.stats.Close()
//    }
//
// The Registry type also allows observers to obtain View instances which
// allows iteration over the contents of the registry.
//
//   view := stats.Root.Open()
//   defer view.Close()
//
//   view.Do(
//      func(s Statistics) {
//         // do something with a Statistics object
//         ...
//      }
//   )
//
type Registry interface {

	// Create a new Builder of statistics objects.
	NewBuilder(k string, n string, tags map[string]string) Builder

	// Open a view of all the registered statistics
	Open() View
}

// The View type provides a means for observers of a Registry
// to view the contents of the Registry.
//
// Views are obtained by calling stats.Root.Open().
type View interface {
	// Called to iterate over the registered statistics sets.
	Do(f func(s Statistics)) View

	// Release all the observations held by a view.
	Close()
}

var (
	// ErrStatAlreadyDeclared is the panic that is issued if the same statistic is declared twice.
	ErrStatAlreadyDeclared = errors.New("statistic has already been declared")

	// ErrAlreadyBuilt is the panic that is issued if the Builder.Build() or Builder.MustBuild() method is called more than once on the same Builder.
	ErrAlreadyBuilt = errors.New("builder method must not be called in built state")

	// ErrAlreadyOpen is the panic that is issued if the Built.Open() method is called while the object is still open.
	ErrAlreadyOpen = errors.New("an object cannot be opened when it is already open")

	// ErrStatNotDeclared is the panic that is issued if an attempt is made to record an undeclared statistic.
	ErrStatNotDeclared = errors.New("statistic has not been declared")

	// ErrStatDeclaredWithDifferentType is the panic that is issued if an attempt is made record a statistic declared with a different type.
	ErrStatDeclaredWithDifferentType = errors.New("statistic declared with different type")

	// ErrAlreadyClosed is the panic that is issued if the Built.Close() method is called while the object is still closed.
	ErrAlreadyClosed = errors.New("an object cannot be closed when it is already closed")
)

// A Collection is a collection of Statistics.
type Collection []Statistics
