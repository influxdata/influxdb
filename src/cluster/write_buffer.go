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
		err := self.writer.Write(request)
		if err == nil {
			self.wal.Commit(*request.RequestNumber, self.serverId)
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
		// empty out the buffer before the replay so new writes can buffer while we're replaying
		channelLen := len(self.writes)
		for i := 0; i < channelLen; i++ {
			<-self.writes
		}
		shardIds := make([]uint32, 0)
		for shardId, _ := range self.shardIds {
			shardIds = append(shardIds, shardId)
		}
		self.wal.RecoverServerFromRequestNumber(missedRequest, shardIds, func(request *protocol.Request, shardId uint32) error {
			request.ShardId = &shardId
			self.write(request)
			return nil
		})

		// now make sure that no new writes were dropped. If so, do the replay again from this place.
		select {
		case missedRequest = <-self.stoppedWrites:
			continue
		default:
			return
		}
	}
}
