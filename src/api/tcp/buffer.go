package tcp

import (
	"bytes"
)

type Buffer struct {
	ReadBuffer *bytes.Buffer
	WriteBuffer *bytes.Buffer
}

