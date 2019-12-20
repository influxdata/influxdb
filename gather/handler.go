package gather

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/nats"
	"go.uber.org/zap"
)

// handler implents nats Handler interface.
type handler struct {
	Scraper   Scraper
	Publisher nats.Publisher
	log       *zap.Logger
}

// Process consumes scraper target from scraper target queue,
// call the scraper to gather, and publish to metrics queue.
func (h *handler) Process(s nats.Subscription, m nats.Message) {
	defer m.Ack()

	req := new(influxdb.ScraperTarget)
	err := json.Unmarshal(m.Data(), req)
	if err != nil {
		h.log.Error("Unable to unmarshal json", zap.Error(err))
		return
	}

	ms, err := h.Scraper.Gather(context.TODO(), *req)
	if err != nil {
		h.log.Error("Unable to gather", zap.Error(err))
		return
	}

	// send metrics to recorder queue
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(ms); err != nil {
		h.log.Error("Unable to marshal json", zap.Error(err))
		return
	}

	if err := h.Publisher.Publish(MetricsSubject, buf); err != nil {
		h.log.Error("Unable to publish scraper metrics", zap.Error(err))
		return
	}

}
