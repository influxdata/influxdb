package integrations

import (
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/go-cmp/cmp"
)

func hostAndPort() (string, int) {
	s := httptest.NewServer(nil)
	defer s.Close()

	u, err := url.Parse(s.URL)
	if err != nil {
		panic(err)
	}
	xs := strings.Split(u.Host, ":")
	host := xs[0]
	portStr := xs[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	return host, port

}

func newBoltFile() string {
	f, err := ioutil.TempFile("", "chronograf-bolt-")
	if err != nil {
		panic(err)
	}
	f.Close()

	return f.Name()
}

func jsonEqual(s1, s2 string) (eq bool, err error) {
	var o1, o2 interface{}

	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Equal(o1, o2), nil
}
