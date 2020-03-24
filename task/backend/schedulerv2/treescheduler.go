package schedulerv2

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/btree"
)

const (
	// degreeBtreeScheduled is the btree degree for the btree internal to the tree scheduler.
	// it is purely a performance tuning parameter, but required by github.com/google/btree
	degreeBtreeScheduled = 3 // TODO(docmerlin): find the best number for this, its purely a perf optimization

)

// TreeScheduler is a Scheduler based on a btree.
// It calls Executor in-order per ID.  That means you are guaranteed that for a specific ID,
//
// - The scheduler should, after creation, automatically call ExecutorFunc, when a task should run as defined by its Schedulable.
//
// - the scheduler's should not be able to get into a state where blocks Release and Schedule indefinitely.
//
// - Schedule should add a Schedulable to being scheduled, and Release should remove a task from being scheduled.
//
// - Calling of ExecutorFunc should be serial in time on a per taskID basis. I.E.: the run at 12:00 will go before the run at 12:01.
//
// Design:
//
// The core of the scheduler is a btree keyed by time, a nonce, and a task ID, and a map keyed by task ID and containing a
// nonce and a time (called a uniqueness index from now on).
// The map is to ensure task uniqueness in the tree, so we can replace or delete tasks in the tree.
//
// Scheduling in the tree consists of a main loop that feeds a fixed set of workers, each with their own communication channel.
// Distribution is handled by hashing the TaskID (to ensure uniform distribution) and then distributing over those channels
// evenly based on the hashed ID.  This is to ensure that all tasks of the same ID go to the same worker.
//
//The workers call ExecutorFunc handle any errors and update the LastScheduled time internally and also via the Checkpointer.
//
// The main loop:
//
// The main loop waits on a time.Timer to grab the task with the minimum time.  Once it successfully grabs a task ready
// to trigger, it will start walking the btree from the item nearest
//
// Putting a task into the scheduler:
//
// Adding a task to the scheduler acquires a write lock, grabs the task from the uniqueness map, and replaces the item
// in the uniqueness index and btree.  If new task would trigger sooner than the current soonest triggering task, it
// replaces the Timer when added to the scheduler.  Finally it releases the write lock.
//
// Removing a task from the scheduler:
//
// Removing a task from the scheduler acquires a write lock, deletes the task from the uniqueness index and from the
// btree, then releases the lock.  We do not have to readjust the time on delete, because, if the minimum task isn't
// ready yet, the main loop just resets the timer and keeps going.
type TreeScheduler struct {
	mu            sync.RWMutex
	priorityQueue *btree.BTree
	nextTime      map[ID]int64 // we need this index so we can delete items from the scheduled
	timer         *clock.Timer
	time          clock.Clock
	when          time.Time
	state         SchedulerState

	executor     Executor
	onErr        ErrorFunc
	checkpointer SchedulableService

	batch itemList

	sm *SchedulerMetrics
}

// ErrorFunc is a function for error handling.  It is a good way to inject logging into a TreeScheduler.
type ErrorFunc func(ctx context.Context, taskID ID, scheduledFor time.Time, err error)

type treeSchedulerOptFunc func(t *TreeScheduler) error

// WithOnErrorFn is an option that sets the error function that gets called when there is an error in a TreeScheduler.
// its useful for injecting logging or special error handling.
func WithOnErrorFn(fn ErrorFunc) treeSchedulerOptFunc {
	return func(t *TreeScheduler) error {
		t.onErr = fn
		return nil
	}
}

// WithTime is an optiom for NewScheduler that allows you to inject a clock.Clock from ben johnson's github.com/benbjohnson/clock library, for testing purposes.
func WithTime(t clock.Clock) treeSchedulerOptFunc {
	return func(sch *TreeScheduler) error {
		sch.time = t
		return nil
	}
}

// WithSchedulerMetrics ...
func WithScheduleMetrics(sm *SchedulerMetrics) treeSchedulerOptFunc {
	return func(sch *TreeScheduler) error {
		sch.sm = sm
		return nil
	}
}

var ErrNoExecutor = errors.New("executor must be a non-nil function")

// NewTreeScheduler gives us a new TreeScheduler and SchedulerMetrics when given an  Executor, a SchedulableService, and zero or more options.
// Schedulers should be initialized with this function.
func NewTreeScheduler(executor Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (*TreeScheduler, *SchedulerMetrics, error) {
	s := &TreeScheduler{
		executor:      executor,
		priorityQueue: btree.New(degreeBtreeScheduled),
		nextTime:      map[ID]int64{},
		onErr:         func(_ context.Context, _ ID, _ time.Time, _ error) {},
		time:          clock.New(),
		checkpointer:  checkpointer,
		state:         SchedulerStateReady,
	}

	// apply options
	for i := range opts {
		if err := opts[i](s); err != nil {
			return nil, nil, err
		}
	}

	if s.sm == nil {
		s.sm = NewSchedulerMetrics()
	}
	s.when = time.Time{}
	// Because a stopped timer will wait forever, this allows us to wait for
	// items to be added before triggering.
	s.timer = s.time.Timer(0)
	s.timer.Stop()

	if executor == nil {
		return nil, nil, ErrNoExecutor
	}
	return s, s.sm, nil
}

func (s *TreeScheduler) Process(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.timer.Stop()
			s.state = SchedulerStateStopped
			s.mu.Unlock()
			return
		case <-s.timer.C:
			for s.process(ctx) {
				// early exit
				select {
				case <-ctx.Done():
					s.mu.Lock()
					s.timer.Stop()
					s.state = SchedulerStateStopped
					s.mu.Unlock()
					return
				default:
				}
			}
		}
	}
}

func (s *TreeScheduler) process(ctx context.Context) bool {
	s.mu.Lock()
	defer func() {
		s.state = SchedulerStateWaiting
		s.mu.Unlock()
	}()
	s.state = SchedulerStateProcessing

	min := s.priorityQueue.Min()
	if min == nil { // grab a new item, because there could be a different item at the top of the queue
		s.when = time.Time{}
		return false
	}
	it := min.(Item)
	if ts := s.time.Now().UTC(); it.When().After(ts) {
		s.timer.Reset(ts.Sub(it.When()))
		return false
	}

	iter := s.iterator(ctx, s.time.Now())
	s.priorityQueue.Ascend(iter)
	for i := range s.batch.toDelete {
		delete(s.nextTime, s.batch.toDelete[i].id)
		s.priorityQueue.Delete(s.batch.toDelete[i])
	}
	s.batch.toDelete = s.batch.toDelete[:0]

	for i := range s.batch.toInsert {
		s.nextTime[s.batch.toInsert[i].id] = s.batch.toInsert[i].when
		s.priorityQueue.ReplaceOrInsert(s.batch.toInsert[i])
	}
	s.batch.toInsert = s.batch.toInsert[:0]

	min = s.priorityQueue.Min()
	if min == nil { // grab a new item, because there could be a different item at the top of the queue after processing
		s.when = time.Time{}
		return false
	}
	it = min.(Item)
	s.when = it.When()

	// Timer fired early.
	until := s.when.Sub(s.time.Now())
	if until > 0 {
		s.resetTimer(until) // we can reset without a stop because we know it is fired here
		return false
	}

	return true
}

// itemList is a list of items for deleting and inserting.  We have to do them seperately instead of just a re-add,
// because usually the items key must be changed between the delete and insert
type itemList struct {
	toInsert []Item
	toDelete []Item
}

func (s *TreeScheduler) resetTimer(whenFromNow time.Duration) {
	s.when = s.time.Now().Add(whenFromNow)
	s.timer.Reset(whenFromNow)
}

func (s *TreeScheduler) iterator(ctx context.Context, ts time.Time) btree.ItemIterator {
	return func(i btree.Item) bool {
		if i == nil {
			return false
		}

		// we want it to panic if things other than Items are populating the
		// scheduler, as it is something we can't recover from.

		it := i.(Item)
		if time.Unix(it.next+it.Offset, 0).UTC().After(ts.UTC()) {
			return false
		}

		t := time.Unix(it.next, 0)
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &ErrUnrecoverable{errors.New("executor panicked")}
				}
			}()
			// report the difference between when the item was supposed to be
			// scheduled and now
			s.sm.reportScheduleDelay(time.Since(it.Next()))
			preExec := time.Now().UTC()
			// execute
			err = s.executor.Execute(ctx, it.id, t, it.When())
			// report how long execution took
			s.sm.reportExecution(err, time.Since(preExec))
			return err
		}()
		if err != nil {
			s.onErr(ctx, it.id, it.Next(), err)
		}
		if err := s.checkpointer.UpdateLastScheduled(ctx, it.id, t); err != nil {
			s.onErr(ctx, it.id, it.Next(), err)
		}

		s.batch.toDelete = append(s.batch.toDelete, it)
		if err := it.updateNext(); err != nil {
			// in this error case we can't schedule next, so we have to drop the task
			s.onErr(context.Background(), it.id, it.Next(), &ErrUnrecoverable{err})
			return true
		}
		s.batch.toInsert = append(s.batch.toInsert, it)

		// early exit if context is canceled.
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
}

// When gives us the next time the scheduler will run a task.
func (s *TreeScheduler) When() time.Time {
	s.mu.RLock()
	w := s.when
	s.mu.RUnlock()
	return w
}

func (s *TreeScheduler) release(taskID ID) {
	when, ok := s.nextTime[taskID]
	if !ok {
		return
	}

	// delete the old task run time
	s.priorityQueue.Delete(Item{id: taskID, when: when})
	delete(s.nextTime, taskID)
}

// Release releases a task.
// Release also cancels the running task.
// Task deletion would be faster if the tree supported deleting ranges.
func (s *TreeScheduler) Release(taskID ID) error {
	s.sm.release(taskID)
	s.mu.Lock()
	s.release(taskID)
	s.mu.Unlock()
	return nil
}

// Schedule put puts a Schedulable on the TreeScheduler.
func (s *TreeScheduler) Schedule(sch Schedulable) error {
	s.sm.schedule(sch.ID())
	it := Item{
		cron:   sch.Schedule(),
		id:     sch.ID(),
		Offset: int64(sch.Offset().Seconds()),
		//last:   sch.LastScheduled().Unix(),
	}
	nt, err := it.cron.Next(sch.LastScheduled())
	if err != nil {
		s.sm.scheduleFail(it.id)
		s.onErr(context.Background(), it.id, time.Time{}, err)
		return err
	}
	it.next = nt.UTC().Unix()
	it.when = it.next + it.Offset

	s.mu.Lock()
	defer s.mu.Unlock()

	nt = nt.Add(sch.Offset())
	if s.when.IsZero() || s.when.After(nt) {
		s.when = nt
		s.timer.Stop()
		until := s.when.Sub(s.time.Now())
		if until <= 0 {
			s.timer.Reset(0)
		} else {
			s.timer.Reset(s.when.Sub(s.time.Now()))
		}
	}
	nextTime, ok := s.nextTime[it.id]

	if ok {
		// delete the old task run time
		s.priorityQueue.Delete(Item{
			when: nextTime,
			id:   it.id,
		})
	}
	s.nextTime[it.id] = it.next + it.Offset

	// insert the new task run time
	s.priorityQueue.ReplaceOrInsert(it)
	return nil
}

func (s *TreeScheduler) State() SchedulerState {
	return SchedulerStateStopped
}

// Item is a task in the scheduler.
type Item struct {
	when   int64
	id     ID
	cron   Schedule
	next   int64
	Offset int64
}

func (it Item) Next() time.Time {
	return time.Unix(it.next, 0).UTC()
}

func (it Item) When() time.Time {
	return time.Unix(it.when, 0).UTC()
}

// Less tells us if one Item is less than another
func (it Item) Less(bItem btree.Item) bool {
	it2 := bItem.(Item)
	return it.when < it2.when || ((it.when == it2.when) && it.id < it2.id)
}

func (it *Item) updateNext() error {
	newNext, err := it.cron.Next(time.Unix(it.next, 0))
	if err != nil {
		return err
	}
	it.next = newNext.UTC().Unix()
	it.when = it.next + it.Offset
	return nil
}
