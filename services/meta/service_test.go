package meta

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

func TestService_Open(t *testing.T) {
	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := NewService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestService_PingEndpoint(t *testing.T) {
	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := NewService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	url, err := url.Parse(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Head("http://" + url.String() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(body))

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestService_LongPollCache(t *testing.T) {
	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := NewService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	url, err := url.Parse(s.URL())
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []byte)

	reqFunc := func(index int) {
		println("here1 ******")
		u := fmt.Sprintf("http://%s?index=%d", url, index)
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		println("here2 ******")

		buf := make([]byte, 10)
		n, err := resp.Body.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		println(string(buf[:n]))
		println("here3 ******")
		ch <- buf[:n]
	}

	go reqFunc(0)
	go reqFunc(0)
	go func() {
		time.Sleep(1 * time.Second)
		//s.handler.store.SetCache([]byte("world"))
	}()

	for n := 0; n < 2; n++ {
		b := <-ch
		data := &Data{}
		if err := data.UnmarshalBinary(b); err != nil {
			t.Fatal(err)
		}
		t.Log(data)
		println("client read cache update")
	}
	close(ch)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func newConfig() *Config {
	cfg := NewConfig()
	cfg.RaftBindAddress = "127.0.0.1:0"
	cfg.HTTPdBindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
	return cfg
}

func testTempDir(skip int) string {
	// Get name of the calling function.
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())
	// Make a temp dir prefixed with calling function's name.
	dir, err := ioutil.TempDir("/tmp", prefix)
	if err != nil {
		panic(err)
	}
	return dir
}
