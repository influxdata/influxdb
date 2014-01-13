package engine

import (
	"protocol"
)

type LocalMapJob struct {
	databaseQuery *DatabaseQuery
	resultStream  engine.MapResultStream
}

func NewLocalMapJob(databaseQuery *DatabaseQuery, resultStream MapResultStream) MapJob {
	return &LocalMapJob{databaseQuery: databaseQuery, resultStream: resultStream}
}

func (self *LocalMapJob) YieldPoint(series *string, point *protocol.Point) (stopQuery *bool, err error) {
	return false, nil
}

func (self *LocalMapJob) Close() {

}

type LocalMapResultStream struct {
	reduceJob ReduceJob
}

func NewLocalMapResultStream(reduceJob *ReduceJob) MapResultStream {
	return &LocalMapResultStream{reduceJob: reduceJob}
}

func (self *LocalMapResultStream) Yield(mapResult *protocol.MapResult) error {
	return self.reduceJob.ShuffleAndReduce([]*protocol.MapResult{mapResult})
}

func (self *LocalMapResultStream) Close() {
	self.reduceJob.Close()
}

type LocalReduceJob struct {
	databaseQuery *DatabaseQuery
	resultStream  engine.QueryResultStream
}

func (self *DatastoreMapJob) YieldPoint(series *string, point *protocol.Point) (stopQuery *bool, err error) {
	// do suff, possibly call to resultStream.Yield()
}

type CoordinatorReduceJob struct {
	DatabaseQuery *protocol.DatabaseQuery
	resultStream  engine.QueryResultStream
}

type RequestHandlerMapStream struct {
	// connection that MapResults get written to
	conn net.Conn
}

func (self *RequestHandlerMapStream) Yield(mapResult *protocol.MapResult) error {
	// buffer these and send once they hit a certain size.
}

func (self *RequestHandlerMapStream) Close() {
	// send any buffered results
}

// no op on this one since the request handler won't need to wait
func (self *RequestHandlerReduceJob) WaitForCompletion() error {
	return nil
}
