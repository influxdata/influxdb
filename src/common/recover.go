package common

import (
	"fmt"
	"os"
	"runtime"
)

func RecoverFunc(database, query string, cleanup func(err interface{})) {
	if err := recover(); err != nil {
		fmt.Fprintf(os.Stderr, "********************************BUG********************************\n")
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		fmt.Fprintf(os.Stderr, "Database: %s\n", database)
		fmt.Fprintf(os.Stderr, "Query: [%s]\n", query)
		fmt.Fprintf(os.Stderr, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
		err = NewQueryError(InternalError, "Internal Error")
		if cleanup != nil {
			cleanup(err)
		}
	}
}
