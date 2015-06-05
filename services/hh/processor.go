package hh

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/influxdb/influxdb/tsdb"
)

type Processor struct {
	mu sync.RWMutex

	dir            string
	maxSize        int64
	maxAge         time.Duration
	retryRateLimit int64

	queues map[uint64]*queue
	writer shardWriter
}

type ProcessorOptions struct {
	MaxSize        int64
	MaxAge         time.Duration
	RetryRateLimit int64
}

func NewProcessor(dir string, writer shardWriter, options ProcessorOptions) (*Processor, error) {

	p := &Processor{
		dir:    dir,
		queues: map[uint64]*queue{},
		writer: writer,
	}
	p.setOptions(options)

	// Create the root directory if it doesn't already exist.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	if err := p.loadQueues(); err != nil {
		return p, err
	}
	return p, nil
}

func (p *Processor) setOptions(options ProcessorOptions) {
	p.maxSize = DefaultMaxSize
	if options.MaxSize != 0 {
		p.maxSize = options.MaxSize
	}

	p.maxAge = DefaultMaxAge
	if options.MaxAge.Nanoseconds() >= 0 {
		p.maxAge = options.MaxAge
	}

	p.retryRateLimit = DefaultRetryRateLimit
	if options.RetryRateLimit != 0 {
		p.retryRateLimit = options.RetryRateLimit
	}
}

func (p *Processor) loadQueues() error {
	files, err := ioutil.ReadDir(p.dir)
	if err != nil {
		return err
	}

	for _, file := range files {

		nodeID, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			return err
		}

		if _, err := p.addQueue(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) addQueue(nodeID uint64) (*queue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	path := filepath.Join(p.dir, strconv.FormatUint(nodeID, 10))
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}

	queue, err := newQueue(path, p.maxSize)
	if err != nil {
		return nil, err
	}
	if err := queue.Open(); err != nil {
		return nil, err
	}
	p.queues[nodeID] = queue
	return queue, nil
}

func (p *Processor) WriteShard(shardID, ownerID uint64, points []tsdb.Point) error {
	queue, ok := p.queues[ownerID]
	if !ok {
		var err error
		if queue, err = p.addQueue(ownerID); err != nil {
			return err
		}
	}

	b := p.marshalWrite(shardID, points)
	return queue.Append(b)
}

func (p *Processor) Process() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// FIXME: each queue should be processed in its own goroutine
	for nodeID, queue := range p.queues {
		for {
			// Get the current block from the queue
			buf, err := queue.Current()
			if err != nil {
				break
			}

			// unmarshal the byte slice back to shard ID and points
			shardID, points, err := p.unmarshalWrite(buf)
			if err != nil {
				// TODO: If we ever get and error here, we should probably drop the
				// the write and let anti-entropy resolve it.  This would be an urecoverable
				// error and could block the queue indefinitely.
				return err
			}

			// Try to send the write to the node
			if err := p.writer.WriteShard(shardID, nodeID, points); err != nil {
				break
			}

			// If we get here, the write succeed to advance the queue to the next item
			if err := queue.Advance(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Processor) marshalWrite(shardID uint64, points []tsdb.Point) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, shardID)
	for _, p := range points {
		b = append(b, []byte(p.String())...)
		b = append(b, '\n')
	}
	return b
}

func (p *Processor) unmarshalWrite(b []byte) (uint64, []tsdb.Point, error) {
	ownerID := binary.BigEndian.Uint64(b[:8])
	points, err := tsdb.ParsePoints(b[8:])
	return ownerID, points, err
}
