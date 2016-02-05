package client

import "time"

// BufferConfig is used to specify how frequently a BufferedClient should flush its dataset to the influxdb server.
// Database: The Database to write points to (gets passed through to BatchPoints).
// FlushMaxPoints: Buffer at most this many points in memory before flushing to the server.
// FlushMaxWaitTime: Buffer points in memory for at most this amount of time before flushing to the server.
type BufferConfig struct {
	Database         string
	FlushMaxPoints   int
	FlushMaxWaitTime time.Duration
}

// NewBufferedClient will instantiate and return a connected BufferedClient.
func NewBufferedClient(clientConfig Config, bufferConfig BufferConfig) (bufferedClient *BufferedClient, err error) {
	client, err := NewClient(clientConfig)
	if err != nil {
		return
	}
	bufferedClient = &BufferedClient{
		Client:       client,
		bufferConfig: bufferConfig,
		ingestChan:   make(chan Point, bufferConfig.FlushMaxPoints/3),
		closeChan:    make(chan chan error, 1),
		flushTimer:   time.NewTimer(bufferConfig.FlushMaxWaitTime),
		pointsBuf:    make([]Point, bufferConfig.FlushMaxPoints),
		pointsIndex:  0,
	}
	go bufferedClient.ingestAndFlushLoop()
	return
}

// BufferedClient is used to buffer points in memory and periodically flush them to the server
type BufferedClient struct {
	*Client
	bufferConfig BufferConfig
	ingestChan   chan Point
	closeChan    chan chan error
	flushTimer   *time.Timer
	pointsBuf    []Point
	pointsIndex  int
}

// Add a Point with the given values to the BufferedClient.
// If the BufferedClient is closed, didAdd is false
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

// Close will close the BufferedClient. While closing, it will flush any points from Add()
// This method executes asynchronously, but it returns a channel which can be read from to ensure that the buffered client
// Once the client
func (b *BufferedClient) Close() error {
	closeResultChan := make(chan error)
	b.closeChan <- closeResultChan
	return <-closeResultChan
}

// Async ingest and flush loop
//////////////////////////////

// Read ingested points, buffer them in memory, and periodically flush to server.
// On Close(), drain ingested points, flush to server, and signal that Close has completed.
func (b *BufferedClient) ingestAndFlushLoop() {
	for b.ingestChan != nil {
		select {
		case point := <-b.ingestChan:
			b.processIngestedPoint(point)
		case <-b.flushTimer.C:
			b.flushBatch()
		case closeResultChan := <-b.closeChan:
			ingestChan := b.ingestChan
			b.ingestChan = nil // At this point b.Add() becomes a no-op and starts returning false
			b.drainChan(ingestChan)
			b.flushBatch()
			b.flushTimer.Stop()
			closeResultChan <- nil
		}
	}
}

// Drain the passed in ingest channel.
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

// Buffer an ingested point into memory.
// Flushes the batch if FlushMaxPoints has been reached.
func (b *BufferedClient) processIngestedPoint(point Point) {
	b.pointsBuf[b.pointsIndex] = point
	b.pointsIndex += 1
	if b.pointsIndex == b.bufferConfig.FlushMaxPoints {
		b.flushBatch()
	}
}

// Flushes all buffered points to the server
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
