package mock

import (
	"net/url"

	"github.com/influxdata/plutonium/meta/control"
)

type ControlClient struct {
	Cluster            *control.Cluster
	ShowClustersCalled bool
}

func NewMockControlClient(addr string) *ControlClient {
	_, err := url.Parse(addr)
	if err != nil {
		panic(err)
	}

	return &ControlClient{
		Cluster: &control.Cluster{
			DataNodes: []control.DataNode{
				control.DataNode{
					HTTPAddr: addr,
				},
			},
		},
	}
}

func (cc *ControlClient) ShowCluster() (*control.Cluster, error) {
	cc.ShowClustersCalled = true
	return cc.Cluster, nil
}
