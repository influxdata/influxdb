package escape

import (
	"testing"
)

var s string

func BenchmarkStringEscapeNoEscapes(b *testing.B) {
	for n := 0; n < b.N; n++ {
		s = String("no_escapes")
	}
}

func BenchmarkStringUnescapeNoEscapes(b *testing.B) {
	for n := 0; n < b.N; n++ {
		s = UnescapeString("no_escapes")
	}
}

func BenchmarkManyStringEscape(b *testing.B) {
	tests := []string{
		"this is my special string",
		"a field w=i th == tons of escapes",
		"some,commas,here",
	}

	for n := 0; n < b.N; n++ {
		for _, test := range tests {
			s = String(test)
		}
	}
}

func BenchmarkManyStringUnescape(b *testing.B) {
	tests := []string{
		`this\ is\ my\ special\ string`,
		`a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`,
		`some\,commas\,here`,
	}

	for n := 0; n < b.N; n++ {
		for _, test := range tests {
			s = UnescapeString(test)
		}
	}
}

func TestStringEscape(t *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{
		{
			in:       "",
			expected: "",
		},
		{
			in:       "this is my special string",
			expected: `this\ is\ my\ special\ string`,
		},
		{
			in:       "a field w=i th == tons of escapes",
			expected: `a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`,
		},
		{
			in:       "no_escapes",
			expected: "no_escapes",
		},
		{
			in:       "some,commas,here",
			expected: `some\,commas\,here`,
		},
	}

	for _, test := range tests {
		if test.expected != String(test.in) {
			t.Errorf("Got %s, expected %s", String(test.in), test.expected)
		}
	}
}

func TestStringUnescape(t *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{
		{
			in:       "",
			expected: "",
		},
		{
			in:       `this\ is\ my\ special\ string`,
			expected: "this is my special string",
		},
		{
			in:       `a\ field\ w\=i\ th\ \=\=\ tons\ of\ escapes`,
			expected: "a field w=i th == tons of escapes",
		},
		{
			in:       "no_escapes",
			expected: "no_escapes",
		},
		{
			in:       `some\,commas\,here`,
			expected: "some,commas,here",
		},
	}

	for _, test := range tests {
		if test.expected != UnescapeString(test.in) {
			t.Errorf("Got %s, expected %s", UnescapeString(test.in), test.expected)
		}
	}
}
