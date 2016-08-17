package models

import (
	"bytes"
	"math/rand"
	"testing"
	"testing/quick"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

func TestUnsafeBytesToString(t *testing.T) {
	f := func(buf []byte) bool {
		gotString := unsafeBytesToString(buf)
		gotBuf := []byte(gotString)

		return bytes.Equal(buf, gotBuf)
	}

	cfg := quick.Config{
		MaxCount: 1000,
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkUnsafeBytesToString_16(b *testing.B) {
	buf := make([]byte, 16)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		unsafeBytesToString(buf)
	}
}

func TestUnsafeStringToBytes(t *testing.T) {
	f := func(s string) bool {
		gotBuf := unsafeStringToBytes(s)
		gotString := string(gotBuf)

		return s == gotString
	}

	cfg := quick.Config{
		MaxCount: 1000,
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkUnsafeStringToBytes_16(b *testing.B) {
	buf := make([]byte, 16)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	s := string(buf)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		unsafeStringToBytes(s)
	}
}
