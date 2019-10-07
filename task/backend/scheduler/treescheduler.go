package scheduler

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cespare/xxhash"
	"github.com/google/btree"
)

const (
	maxWaitTime          = time.Hour
	degreeBtreeScheduled = 3
	defaultMaxWorkers    = 32
)

// TreeScheduler is a Scheduler based on a btree.
// It calls Executor in-order per ID.  That means you are guaranteed that for a specific ID,
//
// If a call to an Executorfunc returns an *ErrRetry then all calls to Executor of the entire task will be delayed
// temporarily by the amount specified in *ErrRetry, but future calls to Executor for that task will proceed normally.
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
	sync.RWMutex
	scheduled    *btree.BTree
	nextTime     map[ID]ordering // we need this index so we can delete items from the scheduled
	when         time.Time
	executor     Executor
	onErr        ErrorFunc
	time         clock.Clock
	timer        *clock.Timer
	done         chan struct{}
	workchans    []chan Item
	wg           sync.WaitGroup
	checkpointer SchedulableService

	sm *SchedulerMetrics
}

type ExecutorFunc func(ctx context.Context, id ID, scheduledAt time.Time) error

type ErrorFunc func(ctx context.Context, taskID ID, scheduledAt time.Time, err error)

type treeSchedulerOptFunc func(t *TreeScheduler) error

func WithOnErrorFn(fn ErrorFunc) treeSchedulerOptFunc {
	return func(t *TreeScheduler) error {
		t.onErr = fn
		return nil
	}
}

func WithMaxConcurrentWorkers(n int) treeSchedulerOptFunc {
	return func(t *TreeScheduler) error {
		t.workchans = make([]chan Item, n)
		return nil
	}
}

func WithTime(t clock.Clock) treeSchedulerOptFunc {
	return func(sch *TreeScheduler) error {
		sch.time = t
		return nil
	}
}

// Executor is any function that accepts an ID, a time, and a duration.
func NewScheduler(executor Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (*TreeScheduler, *SchedulerMetrics, error) {
	s := &TreeScheduler{
		executor:     executor,
		scheduled:    btree.New(degreeBtreeScheduled),
		nextTime:     map[ID]ordering{},
		onErr:        func(_ context.Context, _ ID, _ time.Time, _ error) {},
		time:         clock.New(),
		done:         make(chan struct{}, 1),
		checkpointer: checkpointer,
	}

	// apply options
	for i := range opts {
		if err := opts[i](s); err != nil {
			return nil, nil, err
		}
	}
	if s.workchans == nil {
		s.workchans = make([]chan Item, defaultMaxWorkers)

	}

	s.wg.Add(len(s.workchans))
	for i := 0; i < len(s.workchans); i++ {
		s.workchans[i] = make(chan Item)
		go s.work(i)
	}

	s.sm = NewSchedulerMetrics(s)
	s.when = s.time.Now().Add(maxWaitTime)
	s.timer = s.time.Timer(maxWaitTime)
	if executor == nil {
		return nil, nil, errors.New("Executor must be a non-nil function")
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
	schedulerLoop:
		for {
			select {
			case <-s.done:
				s.Lock()
				s.timer.Stop()
				// close workchans
				for i := range s.workchans {
					close(s.workchans[i])
				}
				s.Unlock()
				return
			case <-s.timer.C:
			fired:
				for {
					s.Lock()
					min := s.scheduled.Min()
					if min == nil { // grab a new item, because there could be a different item at the top of the queue
						s.when = s.time.Now().Add(maxWaitTime)
						s.timer.Reset(maxWaitTime) // we can reset without stop, because its fired.
						s.Unlock()
						continue schedulerLoop
					}
					it := min.(Item)
					if it.when > s.when.UTC().Unix() {
						s.Unlock()
						continue schedulerLoop
					}
					s.process()
					min = s.scheduled.Min()
					if min == nil { // grab a new item, because there could be a different item at the top of the queue after processing
						s.when = s.time.Now().Add(maxWaitTime)
						s.timer.Reset(maxWaitTime) // we can reset without stop, because its fired.
						s.Unlock()
						continue schedulerLoop
					}
					it = min.(Item)
					s.when = time.Unix(it.when, 0)
					until := s.when.Sub(s.time.Now())

					if until > 0 {
						s.timer.Reset(until) // we can reset without stop, because its fired.
						s.Unlock()
						break fired
					}
					s.Unlock()
				}
			}
		}
	}()
	return s, s.sm, nil
}

func (s *TreeScheduler) Stop() {
	s.Lock()
	close(s.done)
	s.Unlock()
	s.wg.Wait()
}

type unsent struct {
	items []Item
}

func (u *unsent) append(i Item) {
	u.items = append(u.items, i)
}

func (s *TreeScheduler) process() {
	iter, toReAdd := s.iterator(s.time.Now())
	s.scheduled.Ascend(iter)
	for i := range toReAdd.items {
		s.nextTime[toReAdd.items[i].id] = toReAdd.items[i].ordering
		s.scheduled.ReplaceOrInsert(toReAdd.items[i])
	}
}

func (s *TreeScheduler) iterator(ts time.Time) (btree.ItemIterator, *unsent) {
	itemsToPlace := &unsent{}
	return func(i btree.Item) bool {
		if i == nil {
			return false
		}
		it := i.(Item) // we want it to panic if things other than Items are populating the scheduler, as it is something we can't recover from.
		if time.Unix(it.next+it.Offset, 0).After(ts) {
			return false
		}
		// distribute to the right worker.
		{
			buf := [8]byte{}
			binary.LittleEndian.PutUint64(buf[:], uint64(it.id))
			wc := xxhash.Sum64(buf[:]) % uint64(len(s.workchans)) // we just hash so that the number is uniformly distributed
			select {
			case s.workchans[wc] <- it:
				s.scheduled.Delete(it)
				if err := it.updateNext(); err != nil {
					s.onErr(context.Background(), it.id, it.Next(), err)
				}
				itemsToPlace.append(it)

			case <-s.done:
				return false
			default:
				s.scheduled.Delete(it)
				it.incrementNonce()
				itemsToPlace.append(it)
				return true
			}
		}
		return true
	}, itemsToPlace
}

func (s *TreeScheduler) Now() time.Time {
	s.RLock()
	now := s.time.Now().UTC()
	s.RUnlock()
	return now
}

// When gives us the next time the scheduler will run a task.
func (s *TreeScheduler) When() time.Time {
	s.RLock()
	w := s.when
	s.RUnlock()
	return w
}

func (s *TreeScheduler) release(taskID ID) {
	ordering, ok := s.nextTime[taskID]
	if !ok {
		return
	}

	// delete the old task run time
	s.scheduled.Delete(Item{id: taskID, ordering: ordering})
	delete(s.nextTime, taskID)
}

// Release releases a task, if it doesn't own the task it just returns.
// Release also cancels the running task.
// Task deletion would be faster if the tree supported deleting ranges.
func (s *TreeScheduler) Release(taskID ID) {
	s.sm.release(taskID)
	s.Lock()
	s.release(taskID)
	s.Unlock()
}

// work does work and reschedules the work as necessary.
// it handles the resceduling, because we need to be able to reschedule based on executor error
func (s *TreeScheduler) work(i int) {
	var it Item
	defer func() {
		s.wg.Done()
	}()
	for it = range s.workchans[i] {
		t := time.Unix(it.next, 0)
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &ErrUnrecoverable{errors.New("Executor panicked")}
				}
			}()
			return s.executor.Execute(context.Background(), it.id, t)
		}()
		if err != nil {
			s.onErr(context.Background(), it.id, it.Next(), err)
		}
		if err := s.checkpointer.UpdateLastScheduled(context.TODO(), it.id, t); err != nil {
			s.onErr(context.Background(), it.id, it.Next(), err)
		}
	}
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
		return err
	}
	it.next = nt.UTC().Unix()
	it.ordering.when = it.next + it.Offset

	s.Lock()
	defer s.Unlock()

	nt = nt.Add(sch.Offset())
	if s.when.After(nt) {
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
		s.scheduled.Delete(Item{
			ordering: nextTime,
			id:       it.id,
		})
	}
	s.nextTime[it.id] = ordering{when: it.next + it.Offset + it.wait}

	// insert the new task run time
	s.scheduled.ReplaceOrInsert(it)
	return nil
}

var maxItem = Item{
	ordering: ordering{
		when:  math.MaxInt64,
		nonce: int(^uint(0) >> 1),
	},
	id: maxID,
}

type ordering struct {
	when  int64
	nonce int // for retries
}

func (k *ordering) incrementNonce() {
	k.nonce++
}

// Item is a task in the scheduler.
type Item struct {
	ordering
	id     ID
	cron   Schedule
	next   int64
	wait   int64
	Offset int64
}

func (it Item) Next() time.Time {
	return time.Unix(it.next, 0)
}

// Less tells us if one Item is less than another
func (it Item) Less(bItem btree.Item) bool {
	it2 := bItem.(Item)
	return it.when < it2.when || (it.when == it2.when && (it.nonce < it2.nonce || it.nonce == it2.nonce && it.id < it2.id))
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
