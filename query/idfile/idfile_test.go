package idfile_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/platform/query/idfile"
	uuid "github.com/satori/go.uuid"
)

func TestID_New(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	os.Remove(tmpfile.Name())

	_, err = idfile.ID(tmpfile.Name())
	if err != nil {
		t.Errorf("unexpected error with idfile %v", err)
	}
	os.Remove(tmpfile.Name())
}

func TestID_Existing(t *testing.T) {
	u := uuid.NamespaceURL
	id := u.Bytes()
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(id); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	got, err := idfile.ID(tmpfile.Name())
	if err != nil {
		t.Errorf("unexpected error with idfile %v", err)
	}

	want := u.String()
	if got != want {
		t.Errorf("ID %s != %s", got, want)
	}
}
