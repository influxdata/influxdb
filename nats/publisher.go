package nats

import (
	"io"
	"io/ioutil"

	stan "github.com/nats-io/go-nats-streaming"
	"go.uber.org/zap"
)

type Publisher interface {
	// Publish a new message to channel
	Publish(subject string, r io.Reader) error
}

type AsyncPublisher struct {
	ClientID   string
	Connection stan.Conn
	log        *zap.Logger
	Addr       string
}

func NewAsyncPublisher(log *zap.Logger, clientID string, addr string) *AsyncPublisher {
	return &AsyncPublisher{
		ClientID: clientID,
		log:      log,
		Addr:     addr,
	}
}

// Open creates and maintains a connection to NATS server
func (p *AsyncPublisher) Open() error {
	sc, err := stan.Connect(ServerName, p.ClientID, stan.NatsURL(p.Addr))
	if err != nil {
		return err
	}
	p.Connection = sc
	return nil
}

func (p *AsyncPublisher) Publish(subject string, r io.Reader) error {
	if p.Connection == nil {
		return ErrNoNatsConnection
	}

	ah := func(guid string, err error) {
		if err != nil {
			p.log.Info(err.Error())
		}
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	_, err = p.Connection.PublishAsync(subject, data, ah)
	return err
}
