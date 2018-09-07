package gather

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/platform/nats"
	"go.uber.org/zap"
)

// Storage stores the metrics of a time based.
type Storage interface {
	//Subscriber nats.Subscriber
	Record([]Metrics) error
}

// StorageHandler implements nats.Handler interface.
type StorageHandler struct {
	Storage Storage
	Logger  *zap.Logger
}

// Process consumes job queue, and use storage to record.
func (h *StorageHandler) Process(s nats.Subscription, m nats.Message) {
	defer m.Ack()
	ms := make([]Metrics, 0)
	err := json.Unmarshal(m.Data(), &ms)
	if err != nil {
		h.Logger.Error(fmt.Sprintf("storage handler process err: %v", err))
		return
	}
	err = h.Storage.Record(ms)
	if err != nil {
		h.Logger.Error(fmt.Sprintf("storage handler store err: %v", err))
	}
}
