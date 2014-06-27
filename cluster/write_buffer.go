package cluster

import (
	"reflect"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

// Acts as a buffer for writes
type WriteBuffer struct {
	writer                     Writer
	wal                        WAL
	serverId                   uint32
	writes                     chan *protocol.Request
	stoppedWrites              chan uint32
	bufferSize                 int
	shardIds                   map[uint32]bool
	shardLastRequestNumber     map[uint32]uint32
	shardCommitedRequestNumber map[uint32]uint32
	writerInfo                 string
}

type Writer interface {
	Write(request *protocol.Request) error
}

func NewWriteBuffer(writerInfo string, writer Writer, wal WAL, serverId uint32, bufferSize int) *WriteBuffer {
	log.Info("%s: Initializing write buffer with buffer size of %d", writerInfo, bufferSize)
	buff := &WriteBuffer{
		writer:                     writer,
		wal:                        wal,
		serverId:                   serverId,
		writes:                     make(chan *protocol.Request, bufferSize),
		stoppedWrites:              make(chan uint32, 1),
		bufferSize:                 bufferSize,
		shardIds:                   make(map[uint32]bool),
		shardLastRequestNumber:     map[uint32]uint32{},
		shardCommitedRequestNumber: map[uint32]uint32{},
		writerInfo:                 writerInfo,
	}
	go buff.handleWrites()
	return buff
}

func (self *WriteBuffer) ShardsRequestNumber() map[uint32]uint32 {
	return self.shardLastRequestNumber
}

func (self *WriteBuffer) HasUncommitedWrites() bool {
	return !reflect.DeepEqual(self.shardCommitedRequestNumber, self.shardLastRequestNumber)
}

// This method never blocks. It'll buffer writes until they fill the buffer then drop the on the
// floor and let the background goroutine replay from the WAL
func (self *WriteBuffer) Write(request *protocol.Request) {
	self.shardLastRequestNumber[request.GetShardId()] = request.GetRequestNumber()
	select {
	case self.writes <- request:
		log.Debug("Buffering %d:%d for %s", request.GetRequestNumber(), request.GetShardId(), self.writerInfo)
		return
	default:
		select {
		case self.stoppedWrites <- *request.RequestNumber:
			return
		default:
			return
		}
	}
}

func (self *WriteBuffer) handleWrites() {
	for {
		select {
		case requestDropped := <-self.stoppedWrites:
			self.replayAndRecover(requestDropped)
		case request := <-self.writes:
			self.write(request)
		}
	}
}

func (self *WriteBuffer) write(request *protocol.Request) {
	attempts := 0
	for {
		self.shardIds[*request.ShardId] = true
		err := self.writer.Write(request)
		if err == nil {
			requestNumber := request.RequestNumber
			if requestNumber == nil {
				return
			}

			self.shardCommitedRequestNumber[request.GetShardId()] = *requestNumber
			log.Debug("Commiting %d:%d for %s", request.GetRequestNumber(), request.GetShardId(), self.writerInfo)
			self.wal.Commit(*requestNumber, self.serverId)
			return
		}
		if attempts%100 == 0 {
			log.Error("%s: WriteBuffer: error on write to server %d: %s", self.writerInfo, self.serverId, err)
		}
		attempts += 1
		// backoff happens in the writer, just sleep for a small fixed amount of time before retrying
		time.Sleep(time.Millisecond * 100)
	}
}

func (self *WriteBuffer) replayAndRecover(missedRequest uint32) {
	var req *protocol.Request

	// empty out the buffer before the replay so new writes can buffer while we're replaying
	channelLen := len(self.writes)
	// This is the first run through the replay. Start from the start of the write queue
	for i := 0; i < channelLen; i++ {
		r := <-self.writes
		if req == nil {
			req = r
		}
	}

	if req == nil {
		log.Error("%s: REPLAY: emptied channel, but no request set", self.writerInfo)
		return
	}
	log.Debug("%s: REPLAY: Emptied out channel", self.writerInfo)

	shardIds := make([]uint32, 0)
	for shardId := range self.shardIds {
		shardIds = append(shardIds, shardId)
	}

	// while we're behind keep replaying from WAL
	for {
		log.Info("%s: REPLAY: Replaying dropped requests...", self.writerInfo)

		log.Debug("%s: REPLAY: from request %d. Shards: %v", self.writerInfo, req.GetRequestNumber(), shardIds)
		self.wal.RecoverServerFromRequestNumber(*req.RequestNumber, shardIds, func(request *protocol.Request, shardId uint32) error {
			log.Debug("%s: REPLAY: writing request number: %d", self.writerInfo, request.GetRequestNumber())
			req = request
			request.ShardId = &shardId
			self.write(request)
			return nil
		})

		log.Info("%s: REPLAY: Emptying out reqeusts from buffer that we've already replayed", self.writerInfo)
	RequestLoop:
		for {
			select {
			case newReq := <-self.writes:
				if *newReq.RequestNumber == *req.RequestNumber {
					break RequestLoop
				}
			default:
				log.Error("%s: REPLAY: Got to the end of the write buffer without getting to the last written request.", self.writerInfo)
				break RequestLoop
			}
		}

		log.Info("%s: REPLAY: done.", self.writerInfo)

		// now make sure that no new writes were dropped. If so, do the replay again from this place.
		select {
		case <-self.stoppedWrites:
			log.Info("%s: REPLAY: Buffer backed up while replaying, going again.", self.writerInfo)
			continue
		default:
			return
		}
	}
}
