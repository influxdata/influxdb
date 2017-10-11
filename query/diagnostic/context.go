package diagnostic

import "time"

type Context interface {
	PrintStatement(query string)
	QueryPanic(query string, err interface{}, stack []byte)
	DetectedSlowQuery(query string, qid uint64, database string, threshold time.Duration)
}
