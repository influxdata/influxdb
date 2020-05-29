package inmem_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/inmem"
)

func TestSessionSet(t *testing.T) {
	st := inmem.NewSessionStore()
	err := st.Set("hi", "friend", time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	err = st.Set("hi", "enemy", time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	word, err := st.Get("hi")
	if err != nil {
		t.Fatal(err)
	}

	if word != "enemy" {
		t.Fatalf("got incorrect response: got %s expected: \"enemy\"", word)
	}
}
func TestSessionGet(t *testing.T) {
	st := inmem.NewSessionStore()
	err := st.Set("hi", "friend", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	word, err := st.Get("hi")
	if err != nil {
		t.Fatal(err)
	}

	if word != "friend" {
		t.Fatalf("got incorrect response: got %s expected: \"enemy\"", word)
	}

	time.Sleep(time.Second * 2)

	word, err = st.Get("hi")
	if err != nil {
		t.Fatal(err)
	}

	if word != "" {
		t.Fatalf("expected no words back but got: %s", word)
	}
}
func TestSessionDelete(t *testing.T) {
	st := inmem.NewSessionStore()
	err := st.Set("hi", "friend", time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Delete("hi"); err != nil {
		t.Fatal(err)
	}

	word, err := st.Get("hi")
	if err != nil {
		t.Fatal(err)
	}

	if word != "" {
		t.Fatalf("expected no words back but got: %s", word)
	}

	if err := st.Delete("hi"); err != nil {
		t.Fatal(err)
	}
}
func TestSessionExpireAt(t *testing.T) {
	st := inmem.NewSessionStore()
	err := st.Set("hi", "friend", time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	if err := st.ExpireAt("hi", time.Now().Add(-20)); err != nil {
		t.Fatal(err)
	}

	word, err := st.Get("hi")
	if err != nil {
		t.Fatal(err)
	}

	if word != "" {
		t.Fatalf("expected no words back but got: %s", word)
	}

	if err := st.Set("hello", "friend", time.Time{}); err != nil {
		t.Fatal(err)
	}

	if err := st.ExpireAt("hello", time.Now()); err != nil {
		t.Fatal(err)
	}

	word, err = st.Get("hello")
	if err != nil {
		t.Fatal(err)
	}

	if word != "" {
		t.Fatalf("expected no words back but got: %s", word)
	}

	if err := st.Set("yo", "friend", time.Time{}); err != nil {
		t.Fatal(err)
	}

	if err := st.ExpireAt("yo", time.Now().Add(100*time.Microsecond)); err != nil {
		t.Fatal(err)
	}

	word, err = st.Get("yo")
	if err != nil {
		t.Fatal(err)
	}

	if word != "friend" {
		t.Fatalf("expected no words back but got: %q", word)
	}

	// add more time to a key
	if err := st.ExpireAt("yo", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	word, err = st.Get("yo")
	if err != nil {
		t.Fatal(err)
	}

	if word != "friend" {
		t.Fatalf("expected key to still exist but got: %q", word)
	}

	time.Sleep(time.Second)

	word, err = st.Get("yo")
	if err != nil {
		t.Fatal(err)
	}

	if word != "" {
		t.Fatalf("expected no words back but got: %s", word)
	}
}
