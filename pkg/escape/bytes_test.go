package escape

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

var result []byte

func BenchmarkBytesEscapeNoEscapes(b *testing.B) {
	buf := []byte(`no_escapes`)
	for i := 0; i < b.N; i++ {
		result = Bytes(buf)
	}
}

func BenchmarkUnescapeNoEscapes(b *testing.B) {
	buf := []byte(`no_escapes`)
	for i := 0; i < b.N; i++ {
		result = Unescape(buf)
	}
}

func BenchmarkBytesEscapeMany(b *testing.B) {
	tests := [][]byte{
		[]byte("this is my special string"),
		[]byte("a field w=i th == tons of escapes"),
		[]byte("some,commas,here"),
	}
	for n := 0; n < b.N; n++ {
		for _, test := range tests {
			result = Bytes(test)
		}
	}
}

func BenchmarkUnescapeMany(b *testing.B) {
	tests := [][]byte{
		[]byte(`this\ is\ my\ special\ string`),
		[]byte(`a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`),
		[]byte(`some\,commas\,here`),
	}
	for i := 0; i < b.N; i++ {
		for _, test := range tests {
			result = Unescape(test)
		}
	}
}

var boolResult bool

func BenchmarkIsEscaped(b *testing.B) {
	tests := [][]byte{
		[]byte(`no_escapes`),
		[]byte(`a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`),
		[]byte(`some\,commas\,here`),
	}
	for i := 0; i < b.N; i++ {
		for _, test := range tests {
			boolResult = IsEscaped(test)
		}
	}
}

func BenchmarkAppendUnescaped(b *testing.B) {
	tests := [][]byte{
		[]byte(`this\ is\ my\ special\ string`),
		[]byte(`a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`),
		[]byte(`some\,commas\,here`),
	}
	for i := 0; i < b.N; i++ {
		result = nil
		for _, test := range tests {
			result = AppendUnescaped(result, test)
		}
	}
}

func TestUnescape(t *testing.T) {
	tests := []struct {
		in  []byte
		out []byte
	}{
		{
			[]byte(nil),
			[]byte(nil),
		},

		{
			[]byte(""),
			[]byte(nil),
		},

		{
			[]byte("\\,\\\"\\ \\="),
			[]byte(",\" ="),
		},

		{
			[]byte("\\\\"),
			[]byte("\\\\"),
		},

		{
			[]byte("plain and simple"),
			[]byte("plain and simple"),
		},
	}

	for ii, tt := range tests {
		got := Unescape(tt.in)
		if !reflect.DeepEqual(got, tt.out) {
			t.Errorf("[%d] Unescape(%#v) = %#v, expected %#v", ii, string(tt.in), string(got), string(tt.out))
		}
	}
}

func TestAppendUnescaped(t *testing.T) {
	cases := strings.Split(strings.TrimSpace(`
normal
inv\alid
goo\"d
sp\ ace
\,\"\ \=
f\\\ x
`), "\n")

	for _, c := range cases {
		exp := Unescape([]byte(c))
		got := AppendUnescaped(nil, []byte(c))

		if !bytes.Equal(got, exp) {
			t.Errorf("AppendUnescaped failed for %#q: got %#q, exp %#q", c, got, exp)
		}
	}

}
