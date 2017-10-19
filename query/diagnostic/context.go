package diagnostic

import "time"

type Handler interface {
	ExecutorHandler
	TaskManagerHandler
}

type ExecutorContext struct {
	Handler ExecutorHandler
}

type ExecutorHandler interface {
	OnExecuteStatement(query string)
	OnPanic(query string, err interface{}, stack []byte)
}

func (c *ExecutorContext) OnExecuteStatement(query string) {
	if c.Handler != nil {
		c.Handler.OnExecuteStatement(query)
	}
}

func (c *ExecutorContext) OnPanic(query string, err interface{}, stack []byte) {
	if c.Handler != nil {
		c.Handler.OnPanic(query, err, stack)
	}
}

type TaskManagerContext struct {
	Handler TaskManagerHandler
}

type TaskManagerHandler interface {
	OnSlowQueryDetected(query string, qid uint64, database string, threshold time.Duration)
}

func (c *TaskManagerContext) OnSlowQueryDetected(query string, qid uint64, database string, threshold time.Duration) {
	if c.Handler != nil {
		c.Handler.OnSlowQueryDetected(query, qid, database, threshold)
	}
}
