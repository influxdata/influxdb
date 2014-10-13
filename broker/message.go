package broker

import (
	"encoding/binary"
	"io"
)

// MessageType represents the type of message.
type MessageType uint16

const (
	ConfigMessageType = 1 << 15
)

const (
	CreateTopicMessageType = ConfigMessageType | MessageType(0x00)
	DeleteTopicMessageType = ConfigMessageType | MessageType(0x01)

	CreateReplicaMessageType = ConfigMessageType | MessageType(0x10)
	DeleteReplicaMessageType = ConfigMessageType | MessageType(0x11)

	SubscribeMessageType   = ConfigMessageType | MessageType(0x20)
	UnsubscribeMessageType = ConfigMessageType | MessageType(0x21)
)

// Message represents a single item in a topic.
type Message struct {
	Type  MessageType
	Index uint64
	Data  []byte
}

// WriteTo encodes and writes the message to a writer. Implements io.WriterTo.
func (m *Message) WriteTo(w io.Writer) (n int, err error) {
	if n, err := w.Write(m.header()); err != nil {
		return n, err
	}
	if n, err := w.Write(m.Data); err != nil {
		return messageHeaderSize + n, err
	}
	return messageHeaderSize + len(m.Data), nil
}

// header returns a byte slice with the message header.
func (m *Message) header() []byte {
	b := make([]byte, messageHeaderSize)
	binary.BigEndian.PutUint16(b[0:2], uint16(m.Type))
	binary.BigEndian.PutUint64(b[2:10], m.Index)
	binary.BigEndian.PutUint32(b[10:14], uint32(len(m.Data)))
	return b
}

// The size of the encoded message header, in bytes.
const messageHeaderSize = 2 + 8 + 4

// MessageDecoder decodes messages from a reader.
type MessageDecoder struct {
	r io.Reader
}

// NewMessageDecoder returns a new instance of the MessageDecoder.
func NewMessageDecoder(r io.Reader) *MessageDecoder {
	return &MessageDecoder{r: r}
}

// Decode reads a message from the decoder's reader.
func (dec *MessageDecoder) Decode(m *Message) error {
	var b [messageHeaderSize]byte
	if _, err := io.ReadFull(dec.r, b[:]); err != nil {
		return err
	}
	m.Type = MessageType(binary.BigEndian.Uint16(b[0:2]))
	m.Index = binary.BigEndian.Uint64(b[2:10])
	m.Data = make([]byte, binary.BigEndian.Uint32(b[10:14]))

	// Read data.
	if _, err := io.ReadFull(dec.r, m.Data); err != nil {
		return err
	}

	return nil
}
