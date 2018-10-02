package mock

import (
	"bytes"
	"sort"
	"sync"
	"testing"

	"github.com/influxdata/platform/nats"
)

// TestNats use the mocked nats publisher and subscriber
// try to collect 0~total integers
func TestNats(t *testing.T) {
	total := 30
	workers := 3
	publisher, subscriber := NewNats()
	subject := "default"
	h := &fakeNatsHandler{
		collector: make([]int, 0),
		totalJobs: make(chan struct{}, total),
	}
	for i := 0; i < workers; i++ {
		subscriber.Subscribe(subject, "", h)
	}

	for i := 0; i < total; i++ {
		buf := new(bytes.Buffer)
		buf.Write([]byte{uint8(i)})
		go publisher.Publish(subject, buf)
	}

	// make sure all done
	for i := 0; i < total; i++ {
		<-h.totalJobs
	}

	sort.Slice(h.collector, func(i, j int) bool {
		return h.collector[i] < h.collector[j]
	})

	for i := 0; i < total; i++ {
		if h.collector[i] != i {
			t.Fatalf("nats mocking got incorrect result, want %d, got %d", i, h.collector[i])
		}
	}
}

type fakeNatsHandler struct {
	sync.Mutex
	collector []int
	totalJobs chan struct{}
}

func (h *fakeNatsHandler) Process(s nats.Subscription, m nats.Message) {
	h.Lock()
	defer h.Unlock()
	defer m.Ack()
	i := m.Data()
	h.collector = append(h.collector, int(i[0]))
	h.totalJobs <- struct{}{}
}
