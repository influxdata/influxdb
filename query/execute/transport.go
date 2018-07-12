package execute

import (
	"sync"
	"sync/atomic"

	"github.com/influxdata/platform/query"
)

type Transport interface {
	Transformation
	// Finished reports when the Transport has completed and there is no more work to do.
	Finished() <-chan struct{}
}

// consecutiveTransport implements Transport by transporting data consecutively to the downstream Transformation.
type consecutiveTransport struct {
	dispatcher Dispatcher

	t        Transformation
	messages MessageQueue

	finished chan struct{}
	errMu    sync.Mutex
	errValue error

	schedulerState int32
	inflight       int32
}

func newConescutiveTransport(dispatcher Dispatcher, t Transformation) *consecutiveTransport {
	return &consecutiveTransport{
		dispatcher: dispatcher,
		t:          t,
		// TODO(nathanielc): Have planner specify message queue initial buffer size.
		messages: newMessageQueue(64),
		finished: make(chan struct{}),
	}
}

func (t *consecutiveTransport) setErr(err error) {
	t.errMu.Lock()
	t.errValue = err
	t.errMu.Unlock()
}
func (t *consecutiveTransport) err() error {
	t.errMu.Lock()
	err := t.errValue
	t.errMu.Unlock()
	return err
}

func (t *consecutiveTransport) Finished() <-chan struct{} {
	return t.finished
}

func (t *consecutiveTransport) RetractTable(id DatasetID, key query.GroupKey) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&retractTableMsg{
		srcMessage: srcMessage(id),
		key:        key,
	})
	return nil
}

func (t *consecutiveTransport) Process(id DatasetID, tbl query.Table) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&processMsg{
		srcMessage: srcMessage(id),
		table:      tbl,
	})
	return nil
}

func (t *consecutiveTransport) UpdateWatermark(id DatasetID, time Time) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&updateWatermarkMsg{
		srcMessage: srcMessage(id),
		time:       time,
	})
	return nil
}

func (t *consecutiveTransport) UpdateProcessingTime(id DatasetID, time Time) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&updateProcessingTimeMsg{
		srcMessage: srcMessage(id),
		time:       time,
	})
	return nil
}

func (t *consecutiveTransport) Finish(id DatasetID, err error) {
	select {
	case <-t.finished:
		return
	default:
	}
	t.pushMsg(&finishMsg{
		srcMessage: srcMessage(id),
		err:        err,
	})
}

func (t *consecutiveTransport) pushMsg(m Message) {
	t.messages.Push(m)
	atomic.AddInt32(&t.inflight, 1)
	t.schedule()
}

const (
	// consecutiveTransport schedule states
	idle int32 = iota
	running
	finished
)

// schedule indicates that there is work available to schedule.
func (t *consecutiveTransport) schedule() {
	if t.tryTransition(idle, running) {
		t.dispatcher.Schedule(t.processMessages)
	}
}

// tryTransition attempts to transition into the new state and returns true on success.
func (t *consecutiveTransport) tryTransition(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&t.schedulerState, old, new)
}

// transition sets the new state.
func (t *consecutiveTransport) transition(new int32) {
	atomic.StoreInt32(&t.schedulerState, new)
}

func (t *consecutiveTransport) processMessages(throughput int) {
PROCESS:
	i := 0
	for m := t.messages.Pop(); m != nil; m = t.messages.Pop() {
		atomic.AddInt32(&t.inflight, -1)
		if f, err := processMessage(t.t, m); err != nil || f {
			// Set the error if there was any
			t.setErr(err)

			// Transition to the finished state.
			if t.tryTransition(running, finished) {
				// Call Finish if we have not already
				if !f {
					t.t.Finish(m.SrcDatasetID(), err)
				}
				// We are finished
				close(t.finished)
				return
			}
		}
		i++
		if i >= throughput {
			// We have done enough work.
			// Transition to the idle state and reschedule for later.
			t.transition(idle)
			t.schedule()
			return
		}
	}

	t.transition(idle)
	// Check if more messages arrived after the above loop finished.
	// This check must happen in the idle state.
	if atomic.LoadInt32(&t.inflight) > 0 {
		if t.tryTransition(idle, running) {
			goto PROCESS
		} // else we have already been scheduled again, we can return
	}
}

// processMessage processes the message on t.
// The return value is true if the message was a FinishMsg.
func processMessage(t Transformation, m Message) (finished bool, err error) {
	switch m := m.(type) {
	case RetractTableMsg:
		err = t.RetractTable(m.SrcDatasetID(), m.Key())
	case ProcessMsg:
		b := m.Table()
		err = t.Process(m.SrcDatasetID(), b)
		b.RefCount(-1)
	case UpdateWatermarkMsg:
		err = t.UpdateWatermark(m.SrcDatasetID(), m.WatermarkTime())
	case UpdateProcessingTimeMsg:
		err = t.UpdateProcessingTime(m.SrcDatasetID(), m.ProcessingTime())
	case FinishMsg:
		t.Finish(m.SrcDatasetID(), m.Error())
		finished = true
	}
	return
}

type Message interface {
	Type() MessageType
	SrcDatasetID() DatasetID
}

type MessageType int

const (
	RetractTableType MessageType = iota
	ProcessType
	UpdateWatermarkType
	UpdateProcessingTimeType
	FinishType
)

type srcMessage DatasetID

func (m srcMessage) SrcDatasetID() DatasetID {
	return DatasetID(m)
}

type RetractTableMsg interface {
	Message
	Key() query.GroupKey
}

type retractTableMsg struct {
	srcMessage
	key query.GroupKey
}

func (m *retractTableMsg) Type() MessageType {
	return RetractTableType
}
func (m *retractTableMsg) Key() query.GroupKey {
	return m.key
}

type ProcessMsg interface {
	Message
	Table() query.Table
}

type processMsg struct {
	srcMessage
	table query.Table
}

func (m *processMsg) Type() MessageType {
	return ProcessType
}
func (m *processMsg) Table() query.Table {
	return m.table
}

type UpdateWatermarkMsg interface {
	Message
	WatermarkTime() Time
}

type updateWatermarkMsg struct {
	srcMessage
	time Time
}

func (m *updateWatermarkMsg) Type() MessageType {
	return UpdateWatermarkType
}
func (m *updateWatermarkMsg) WatermarkTime() Time {
	return m.time
}

type UpdateProcessingTimeMsg interface {
	Message
	ProcessingTime() Time
}

type updateProcessingTimeMsg struct {
	srcMessage
	time Time
}

func (m *updateProcessingTimeMsg) Type() MessageType {
	return UpdateProcessingTimeType
}
func (m *updateProcessingTimeMsg) ProcessingTime() Time {
	return m.time
}

type FinishMsg interface {
	Message
	Error() error
}

type finishMsg struct {
	srcMessage
	err error
}

func (m *finishMsg) Type() MessageType {
	return FinishType
}
func (m *finishMsg) Error() error {
	return m.err
}
