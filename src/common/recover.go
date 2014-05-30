package common

import (
	"bytes"
	"fmt"
	"runtime"

	log "code.google.com/p/log4go"
)

func RecoverFunc(database, query string, cleanup func(err interface{})) {
	if err := recover(); err != nil {
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		b := bytes.NewBufferString("")
		fmt.Fprintf(b, "********************************BUG********************************\n")
		fmt.Fprintf(b, "Database: %s\n", database)
		fmt.Fprintf(b, "Query: [%s]\n", query)
		fmt.Fprintf(b, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
		log.Error(b.String())
		err = NewQueryError(InternalError, "Internal Error: %s", err)
		if cleanup != nil {
			cleanup(err)
		}
	}
}
