package iocounter

import (
	"io"
)

// Writer is counter for io.Writer
type Writer struct {
	io.Writer
	count int64
}

func (c *Writer) Write(buf []byte) (int, error) {
	n, err := c.Writer.Write(buf)
	c.count += int64(n)
	return n, err
}

// Count function return counted bytes
func (c *Writer) Count() int64 {
	return c.count
}
