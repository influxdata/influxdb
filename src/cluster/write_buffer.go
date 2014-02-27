package cluster

import (
	log "code.google.com/p/log4go"
	"protocol"
	"time"
)

// Acts as a buffer for writes
type WriteBuffer struct {
	writer        Writer
	wal           WAL
	serverId      uint32
	writes        chan *protocol.Request
	stoppedWrites chan uint32
	bufferSize    int
	shardIds      map[uint32]bool
}

type Writer interface {
	Write(request *protocol.Request) error
}

func NewWriteBuffer(writer Writer, wal WAL, serverId uint32, bufferSize int) *WriteBuffer {
	log.Info("Initializing write buffer with buffer size of %d", bufferSize)
	buff := &WriteBuffer{
		writer:        writer,
		wal:           wal,
		serverId:      serverId,
		writes:        make(chan *protocol.Request, bufferSize),
		stoppedWrites: make(chan uint32, 1),
		bufferSize:    bufferSize,
		shardIds:      make(map[uint32]bool),
	}
	go buff.handleWrites()
	return buff
}

// This method never blocks. It'll buffer writes until they fill the buffer then drop the on the
// floor and let the background goroutine replay from the WAL
func (self *WriteBuffer) Write(request *protocol.Request) {
	select {
	case self.writes <- request:
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
		requestNumber := *request.RequestNumber
		err := self.writer.Write(request)
		if err == nil {
			self.wal.Commit(requestNumber, self.serverId)
			return
		}
		if attempts%100 == 0 {
			log.Error("WriteBuffer: error on write to server %d: %s", self.serverId, err)
		}
		attempts += 1
		// backoff happens in the writer, just sleep for a small fixed amount of time before retrying
		time.Sleep(time.Millisecond * 100)
	}
}

func (self *WriteBuffer) replayAndRecover(missedRequest uint32) {
	for {
		log.Info("REPLAY: Replaying dropped requests...")
		// empty out the buffer before the replay so new writes can buffer while we're replaying
		channelLen := len(self.writes)
		var req *protocol.Request

		// if req is nil, this is the first run through the replay. Start from the start of the write queue
		if req == nil {
			for i := 0; i < channelLen; i++ {
				r := <-self.writes
				if req == nil {
					req = r
				}
			}
		}
		if req == nil {
			log.Error("REPLAY: emptied channel, but no request set")
			return
		}
		log.Info("REPLAY: Emptied out channel")
		shardIds := make([]uint32, 0)
		for shardId, _ := range self.shardIds {
			shardIds = append(shardIds, shardId)
		}

		log.Info("REPLAY: Shards: ", shardIds)
		self.wal.RecoverServerFromRequestNumber(*req.RequestNumber, shardIds, func(request *protocol.Request, shardId uint32) error {
			req = request
			request.ShardId = &shardId
			self.write(request)
			return nil
		})

		log.Info("REPLAY: Emptying out reqeusts from buffer that we've already replayed")
	RequestLoop:
		for {
			select {
			case newReq := <-self.writes:
				if *newReq.RequestNumber == *req.RequestNumber {
					break RequestLoop
				}
			default:
				log.Error("REPLAY: Got to the end of the write buffer without getting to the last written request.")
				break RequestLoop
			}
		}

		log.Info("REPLAY: done.")

		// now make sure that no new writes were dropped. If so, do the replay again from this place.
		select {
		case <-self.stoppedWrites:
			log.Info("REPLAY: Buffer backed up while replaying, going again.")
			continue
		default:
			return
		}
	}
}
