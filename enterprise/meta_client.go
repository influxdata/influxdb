package enterprise

import (
	"encoding/json"
	"net/http"
	"net/url"
)

type MetaClient struct {
	MetaHostPort string
}

func (t *MetaClient) ShowCluster() (*Cluster, error) {
	u := &url.URL{}
	u.Host = t.MetaHostPort
	u.Path = "/show-cluster"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	out := &Cluster{}
	err = dec.Decode(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
