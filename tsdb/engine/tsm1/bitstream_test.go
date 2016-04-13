package tsm1_test

// NOTE(benbjohnson)
// Copied from: github.com/dgryski/go-bitstream
// Required copy so that the underlying reader could be reused without alloc.

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/dgryski/go-bitstream"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestBitStreamEOF(t *testing.T) {
	br := tsm1.NewReader(strings.NewReader("0"))

	b, err := br.ReadByte()
	if b != '0' {
		t.Error("ReadByte didn't return first byte")
	}

	_, err = br.ReadByte()
	if err != io.EOF {
		t.Error("ReadByte on empty string didn't return EOF")
	}

	// 0 = 0b00110000
	br = tsm1.NewReader(strings.NewReader("0"))

	buf := bytes.NewBuffer(nil)
	bw := bitstream.NewWriter(buf)

	for i := 0; i < 4; i++ {
		bit, err := br.ReadBit()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("GetBit returned error err=", err.Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	bw.Flush(bitstream.One)

	err = bw.WriteByte(0xAA)
	if err != nil {
		t.Error("unable to WriteByte")
	}

	c := buf.Bytes()

	if len(c) != 2 || c[1] != 0xAA || c[0] != 0x3f {
		t.Error("bad return from 4 read bytes")
	}

	_, err = tsm1.NewReader(strings.NewReader("")).ReadBit()
	if err != io.EOF {
		t.Error("ReadBit on empty string didn't return EOF")
	}
}

func TestBitStream(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	br := tsm1.NewReader(strings.NewReader("hello"))
	bw := bitstream.NewWriter(buf)

	for {
		bit, err := br.ReadBit()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("GetBit returned error err=", err.Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	s := buf.String()

	if s != "hello" {
		t.Error("expected 'hello', got=", []byte(s))
	}
}

func TestByteStream(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	br := tsm1.NewReader(strings.NewReader("hello"))
	bw := bitstream.NewWriter(buf)

	for i := 0; i < 3; i++ {
		bit, err := br.ReadBit()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("GetBit returned error err=", err.Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	for i := 0; i < 3; i++ {
		byt, err := br.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error("GetByte returned error err=", err.Error())
			return
		}
		bw.WriteByte(byt)
	}

	u, err := br.ReadBits(13)

	if err != nil {
		t.Error("ReadBits returned error err=", err.Error())
		return
	}

	bw.WriteBits(u, 13)

	bw.WriteBits(('!'<<12)|('.'<<4)|0x02, 20)
	// 0x2f == '/'
	bw.Flush(bitstream.One)

	s := buf.String()

	if s != "hello!./" {
		t.Errorf("expected 'hello!./', got=%x", []byte(s))
	}
}
