package iocounter

// Counter counts a number of bytes during an IO operation.
type Counter interface {
	Count() int64
}
