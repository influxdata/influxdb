package meta_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/influxdb/influxdb/services/meta"
)

func TestService_Open(t *testing.T) {
	cfg := newConfig()
	s := meta.NewService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestService_PingEndpoint(t *testing.T) {
	cfg := newConfig()
	s := meta.NewService(cfg)
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
	s := meta.NewService(cfg)
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

	for n := 0; n < 2; n++ {
		b := <-ch
		t.Log(b)
		println("client read cache update")
	}
	close(ch)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func newConfig() *meta.Config {
	cfg := meta.NewConfig()
	cfg.HTTPdBindAddress = "127.0.0.1:0"
	return cfg
}
