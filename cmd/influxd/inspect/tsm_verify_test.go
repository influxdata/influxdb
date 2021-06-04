package inspect

import (
	"bytes"
	"encoding/binary"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestInvalidChecksum(t *testing.T) {
	test := newChecksumTest(t, true)
	defer test.close()

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetArgs([]string{"--dir", test.Path})
	err := verify.Execute()
	test.assertError(err)
}

func TestValidChecksum(t *testing.T) {
	test := newChecksumTest(t, false)
	defer test.close()

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetArgs([]string{"--dir", test.Path})
	err := verify.Execute()
	test.assertNoError(err)

	out, err := ioutil.ReadAll(b)
	test.assertNoError(err)
	test.assert(strings.Contains(string(out), "Broken Blocks: 0 / 1"))
}

func TestInvalidUTF8(t *testing.T) {
	test := newUTFTest(t, true)
	defer test.close()

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetArgs([]string{"--dir", test.Path, "--check-utf8"})
	err := verify.Execute()
	test.assertError(err)
}

func TestValidUTF8(t *testing.T) {
	test := newUTFTest(t, false)
	defer test.close()

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetArgs([]string{"--dir", test.Path, "--check-utf8"})
	err := verify.Execute()
	test.assertNoError(err)

	out, err := ioutil.ReadAll(b)
	test.assertNoError(err)
	test.assert(strings.Contains(string(out), "Invalid Keys: 0 / 1"))
}

func newUTFTest(t *testing.T, withError bool) *Test {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-tsm")
	if err != nil {
		t.Fatal(err)
	}

	f, err := ioutil.TempFile(dir, "verifytsmtest*"+"."+tsm1.TSMFileExtension)
	if err != nil {
		t.Fatal(err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	if withError {
		if err := w.Write([]byte("cpu"), values); err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(f, binary.BigEndian, []byte("foobar\n")); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := w.Write([]byte("cpu"), values); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}

	return &Test{
		T:    t,
		Path: dir,
	}
}

func newChecksumTest(t *testing.T, withError bool) *Test {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-tsm")
	if err != nil {
		t.Fatal(err)
	}

	f, err := ioutil.TempFile(dir, "verifytsmtest*" + "." + tsm1.TSMFileExtension)
	if err != nil {
		t.Fatal(err)
	}

	w, err := tsm1.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	if err := w.Write([]byte("cpu"), values); err != nil {
		t.Fatal(err)
	}

	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}

	if withError {
		fh, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func(fh *os.File) {
			err := fh.Close()
			if err != nil {
				t.Fatal(err)
			}
		}(fh)

		_, err = fh.Write([]byte("BADENTRY"))
		if err != nil {
			t.Fatal(err)
		}

		if err := w.WriteIndex(); err != nil {
			t.Fatal(err)
		}
	}

	return &Test{
		T:    t,
		Path: dir,
	}
}

type Test struct {
	*testing.T
	Path string
}

func (t *Test) close() {
	os.RemoveAll(t.Path)
}

func (t *Test) assertNoError(err error) {
	t.Helper()
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
}

func (t *Test) assert(x bool) {
	t.Helper()
	if !x {
		t.Fatal("unexpected condition")
	}
}

func (t *Test) assertError(err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error and got none")
	}
}

