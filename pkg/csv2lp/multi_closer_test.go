package csv2lp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type testCloser struct {
	errorMsg string
}

func (c testCloser) Close() error {
	if c.errorMsg == "" {
		return nil
	}
	return errors.New(c.errorMsg)
}

// Test_escapeMeasurement
func TestMultiCloser(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	oldFlags := log.Flags()
	log.SetFlags(0)
	oldPrefix := log.Prefix()
	prefix := "::PREFIX::"
	log.SetPrefix(prefix)
	defer func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}()

	var tests = []struct {
		subject io.Closer
		errors  int
	}{
		{MultiCloser(), 0},
		{MultiCloser(testCloser{}, testCloser{}), 0},
		{MultiCloser(testCloser{"a"}, testCloser{}, testCloser{"c"}), 2},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			buf.Reset()
			err := test.subject.Close()
			messages := strings.Count(buf.String(), prefix)
			require.Equal(t, test.errors, messages)
			if test.errors > 0 {
				require.NotNil(t, err)
			}
		})
	}
}
