package execute

import (
	"sync"
	"sync/atomic"
)

// MessageQueue provides a concurrency safe queue for messages.
// The queue must have a single consumer calling Pop.
type MessageQueue interface {
	Push(Message)
	Pop() Message
}

type unboundedMessageQueue struct {
	buf  []Message
	head int
	tail int
	mu   sync.Mutex
	len  int32
}

func newMessageQueue(n int) *unboundedMessageQueue {
	return &unboundedMessageQueue{
		buf: make([]Message, n),
	}
}

func (q *unboundedMessageQueue) Push(m Message) {
	q.mu.Lock()
	size := len(q.buf)
	q.tail = (q.tail + 1) % size
	if q.tail == q.head {
		// Resize
		buf := make([]Message, size*2)
		copy(buf, q.buf[q.head:])
		copy(buf[size-q.head:], q.buf[:q.head])
		q.head = 0
		q.tail = size
		q.buf = buf
	}
	atomic.AddInt32(&q.len, 1)
	q.buf[q.tail] = m
	q.mu.Unlock()
}

func (q *unboundedMessageQueue) Len() int {
	return int(atomic.LoadInt32(&q.len))
}

func (q *unboundedMessageQueue) Pop() Message {
	if q.Len() == 0 {
		return nil
	}

	q.mu.Lock()
	size := len(q.buf)
	q.head = (q.head + 1) % size
	m := q.buf[q.head]
	q.buf[q.head] = nil
	atomic.AddInt32(&q.len, -1)
	q.mu.Unlock()
	return m
}
