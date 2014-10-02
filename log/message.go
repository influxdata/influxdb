package log

import (
	"encoding/binary"
	"io"
)

// The size of the encoded message header, in bytes.
const messageHeaderSize = 2 + 8 + 4

// MessageType represents the type of message.
type MessageType uint16

// Message represents a single item in a topic.
type Message struct {
	Type  MessageType
	Index uint64
	Data  []byte
}

// MessageEncoder encodes messages to a writer.
type MessageEncoder struct {
	w io.Writer
}

// NewMessageEncoder returns a new instance of the MessageEncoder.
func NewMessageEncoder(w io.Writer) *MessageEncoder {
	return &MessageEncoder{w: w}
}

// Encode writes a message to the encoder's writer.
func (enc *MessageEncoder) Encode(m *Message) error {
	// Generate and write message header.
	var b [messageHeaderSize]byte
	binary.BigEndian.PutUint16(b[0:2], uint16(m.Type))
	binary.BigEndian.PutUint64(b[2:10], m.Index)
	binary.BigEndian.PutUint32(b[10:14], uint32(len(m.Data)))
	if _, err := enc.w.Write(b[:]); err != nil {
		return err
	}

	// Write data.
	if _, err := enc.w.Write(m.Data); err != nil {
		return err
	}
	return nil
}

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
