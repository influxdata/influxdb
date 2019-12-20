package tsm1

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/pkg/fs"
)

type keyValues struct {
	key    string
	values []Value
}

func MustTempDir() string {
	dir, err := ioutil.TempDir("", "tsm1-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := ioutil.TempFile(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func newFiles(dir string, values ...keyValues) ([]string, error) {
	var files []string

	id := 1
	for _, v := range values {
		f := MustTempFile(dir)
		w, err := NewTSMWriter(f)
		if err != nil {
			return nil, err
		}

		if err := w.Write([]byte(v.key), v.values); err != nil {
			return nil, err
		}

		if err := w.WriteIndex(); err != nil {
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		newName := filepath.Join(filepath.Dir(f.Name()), DefaultFormatFileName(id, 1)+".tsm")
		if err := fs.RenameFile(f.Name(), newName); err != nil {
			return nil, err
		}
		id++

		files = append(files, newName)
	}
	return files, nil
}

func TestFileStore_DuplicatePoints(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	makeVals := func(ts ...int64) []Value {
		vals := make([]Value, len(ts))
		for i, t := range ts {
			vals[i] = NewFloatValue(t, 1.01)
		}
		return vals
	}

	// Setup 3 files
	data := []keyValues{
		{"m,_field=v#!~#v", makeVals(21)},
		{"m,_field=v#!~#v", makeVals(44)},
		{"m,_field=v#!~#v", makeVals(40, 46)},
		{"m,_field=v#!~#v", makeVals(46, 51)},
	}

	files, err := newFiles(dir, data...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	_ = fs.Replace(nil, files)

	t.Run("ascending", func(t *testing.T) {
		const START, END = 21, 100
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, true)
		defer kc.Close()
		cur := newFloatArrayAscendingCursor()
		cur.reset(START, END, nil, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{21, 40, 44, 46, 51}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

	t.Run("descending", func(t *testing.T) {
		const START, END = 51, 0
		kc := fs.KeyCursor(context.Background(), []byte("m,_field=v#!~#v"), START, false)
		defer kc.Close()
		cur := newFloatArrayDescendingCursor()
		cur.reset(START, END, nil, kc)

		var got []int64
		ar := cur.Next()
		for ar.Len() > 0 {
			got = append(got, ar.Timestamps...)
			ar = cur.Next()
		}

		if exp := []int64{51, 46, 44, 40, 21}; !cmp.Equal(got, exp) {
			t.Errorf("unexpected values; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})
}
