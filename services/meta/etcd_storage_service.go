package meta

import (
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type etcdStorageService struct {
	client *clientv3.Client
}

func newEtcdStorageService(config *Config) (*etcdStorageService, error) {
	endpoints := strings.Split(config.Dir, ",")
	etcdConfig := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, err
	}

	return &etcdStorageService{
		client: client,
	}, nil
}

func (*etcdStorageService) Load() (*Data, error) {
	return nil, nil
}

func (*etcdStorageService) Snapshot(data *Data) error {
	return nil
}
