package client

import "time"

type BufferConfig struct {
	FlushMaxPoints   int
	FlushMaxWaitTime time.Duration
	Database         string
}

func NewBufferedClient(clientConfig Config, bufferConfig BufferConfig) (bufferedClient *BufferedClient, err error) {
	client, err := NewClient(clientConfig)
	if err != nil {
		return
	}
	bufferedClient = &BufferedClient{
		Client:       client,
		bufferConfig: bufferConfig,
		ingestChan:   make(chan Point, bufferConfig.FlushMaxPoints/3),
		closeChan:    make(chan chan bool, 1),
		flushTimer:   time.NewTimer(bufferConfig.FlushMaxWaitTime),
		pointsBuf:    make([]Point, bufferConfig.FlushMaxPoints),
		pointsIndex:  0,
	}
	go bufferedClient.ingestAndFlushLoop()
	return
}

type BufferedClient struct {
	*Client
	bufferConfig BufferConfig
	ingestChan   chan Point
	closeChan    chan chan bool
	flushTimer   *time.Timer
	pointsBuf    []Point
	pointsIndex  int
}

func (b *BufferedClient) Add(measurement string, val interface{}, tags map[string]string, fields map[string]interface{}) (didAdd bool) {
	ingestChan := b.ingestChan
	if ingestChan == nil {
		return
	}
	if fields == nil {
		fields = make(map[string]interface{}, 1)
	}
	fields["value"] = val
	ingestChan <- Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Time:        time.Now(),
	}
	didAdd = true
	return
}

func (b *BufferedClient) Close() (didCloseChan chan bool) {
	didCloseChan = make(chan bool)
	b.closeChan <- didCloseChan
	return didCloseChan
}

// Async ingest and flush loop
//////////////////////////////

func (b *BufferedClient) ingestAndFlushLoop() {
	for b.ingestChan != nil {
		select {
		case point := <-b.ingestChan:
			b.processIngestedPoint(point)
		case <-b.flushTimer.C:
			b.flushBatch()
		case didCloseChan := <-b.closeChan:
			ingestChan := b.ingestChan
			b.ingestChan = nil // At this point b.Add() becomes a no-op and starts returning false
			b.drainChan(ingestChan)
			b.flushBatch()
			didCloseChan <- true
		}
	}
}

func (b *BufferedClient) drainChan(ingestChan chan Point) {
	for {
		select {
		case point := <-ingestChan:
			b.processIngestedPoint(point)
		default:
			return
		}
	}
}

func (b *BufferedClient) processIngestedPoint(point Point) {
	b.pointsBuf[b.pointsIndex] = point
	b.pointsIndex += 1
	if b.pointsIndex == b.bufferConfig.FlushMaxPoints {
		b.flushBatch()
	}
}

func (b *BufferedClient) flushBatch() {
	if b.pointsIndex == 0 {
		return
	}
	b.flushTimer.Stop()
	b.Client.Write(BatchPoints{
		Points:   b.pointsBuf[0:b.pointsIndex],
		Database: b.bufferConfig.Database,
	})
	b.pointsIndex = 0
	b.flushTimer.Reset(b.bufferConfig.FlushMaxWaitTime)
}
