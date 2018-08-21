package gather

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/influxdata/platform/flux"
	"github.com/influxdata/platform/nats"
)

// Storage stores the metrics of a time based
type Storage interface {
	//Subscriber nats.Subscriber
	Record([]Metrics) error
}

// NewNatsStorage use nats to publish each store request
// and the subscriber will use the embeded storage to do the store
// activity
func NewNatsStorage(
	storage Storage,
	subject string,
	logger *log.Logger,
	publisher nats.Publisher,
	subscriber nats.Subscriber,
) Storage {
	s := &natsStorage{
		storage:    storage,
		subject:    subject,
		publisher:  publisher,
		subscriber: subscriber,
	}
	s.subscriber.Subscribe(s.subject, "", &storageHandler{
		storage: storage,
		logger:  logger,
	})
	return s
}

type natsStorage struct {
	storage    Storage
	subject    string
	subscriber nats.Subscriber
	publisher  nats.Publisher
}

func (s *natsStorage) Record(ms []Metrics) error {
	buf := new(bytes.Buffer)
	b, err := json.Marshal(ms)
	if err != nil {
		return fmt.Errorf("scrapper metrics serialization error: %v", err)
	}
	_, err = buf.Write(b)
	if err != nil {
		return fmt.Errorf("scrapper metrics buffer write error: %v", err)
	}
	if err = s.publisher.Publish(s.subject, buf); err != nil {
		return fmt.Errorf("scrapper publisher publish error: %v", err)
	}
	return nil
}

type storageHandler struct {
	storage Storage
	logger  *log.Logger
}

func (h *storageHandler) Process(s nats.Subscription, m nats.Message) {
	defer m.Ack()
	ms := make([]Metrics, 0)
	err := json.Unmarshal(m.Data(), &ms)
	if err != nil {
		h.logger.Printf("storage handler process err: %v\n", err)
	}
	err = h.storage.Record(ms)
	if err != nil {
		h.logger.Printf("storage handler store err: %v\n", err)
	}
}

// NewInfluxStorage create a new influx storage
// which will storage data directly to influxdb
func NewInfluxStorage(urls []string) Storage {
	return &influxStorage{
		URLs: urls,
	}
}

type influxStorage struct {
	URLs   []string
	Client flux.Client
}

func (s *influxStorage) Record(ms []Metrics) error {
	println(s.URLs)
	for _, m := range ms {
		fmt.Println(m)
	}
	return nil
}
