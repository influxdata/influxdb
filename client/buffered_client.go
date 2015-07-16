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
	flushTimer   *time.Timer
	pointsBuf    []Point
	pointsIndex  int
}

func (b *BufferedClient) Add(measurement string, val interface{}, tags map[string]string, fields map[string]interface{}) {
	if fields == nil {
		fields = make(map[string]interface{}, 1)
	}
	fields["value"] = val
	b.ingestChan <- Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Time:        time.Now(),
	}
}

// Async ingest and flush loop
//////////////////////////////

func (b *BufferedClient) ingestAndFlushLoop() {
	for { // loop indefinitely
		select {
		case point := <-b.ingestChan:
			b.pointsBuf[b.pointsIndex] = point
			b.pointsIndex += 1
			if b.pointsIndex == b.bufferConfig.FlushMaxPoints {
				b.flushBatch()
			}
		case <-b.flushTimer.C:
			b.flushBatch()
		}
	}
}

func (b *BufferedClient) flushBatch() {
	b.flushTimer.Stop()
	b.Client.Write(BatchPoints{
		Points:   b.pointsBuf[0:b.pointsIndex],
		Database: b.bufferConfig.Database,
	})
	b.pointsIndex = 0
	b.flushTimer.Reset(b.bufferConfig.FlushMaxWaitTime)
}
