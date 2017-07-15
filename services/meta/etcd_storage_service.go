package meta

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"golang.org/x/net/context"
)

const (
	rootNS        = "/influxdb/"
	userNS        = rootNS + "users/"
	dbNS          = rootNS + "dbs/"
	nodeNS        = rootNS + "nodes/"
	masterNS      = rootNS + "master/"
	masterEpochNS = rootNS + "master_epoch/"
)

type EtcdStorageService struct {
	client  *clientv3.Client
	watcher clientv3.Watcher
	lock    sync.RWMutex
	nsKVs   map[string]clientv3.KV
}

func NewEtcdStorageService(config *Config) (*EtcdStorageService, error) {
	endpoints := strings.Split(config.Dir, ",")
	etcdConfig := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, err
	}

	return &EtcdStorageService{
		client:  client,
		watcher: clientv3.NewWatcher(client),
		nsKVs:   make(map[string]clientv3.KV, 6),
	}, nil
}

func (e *EtcdStorageService) Load() (*Data, error) {
	return nil, nil
}

func (e *EtcdStorageService) Snapshot(data *Data) error {
	return nil
}

// getKV returns namespace KV according to namespace passed in
// if the corresponding namespace KV doesn't exist, create it
func (e *EtcdStorageService) getKV(ns string) clientv3.KV {
	e.lock.RLock()
	if nsKV, ok := e.nsKVs[ns]; ok {
		e.lock.RUnlock()
		return nsKV
	}

	// Release read lock
	e.lock.RUnlock()

	// Hold write lock
	e.lock.Lock()

	// Re-evaluate
	if _, ok := e.nsKVs[ns]; !ok {
		e.nsKVs[ns] = namespace.NewKV(e.client.KV, ns)
	}

	nsKV := e.nsKVs[ns]
	e.lock.Unlock()

	return nsKV
}

func (e *EtcdStorageService) AddUser(user *UserInfo) error {
	nsKV := e.getKV(userNS)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}
	_, err = nsKV.Put(ctx, user.Name, string(data))
	cancel()

	return err
}

func (e *EtcdStorageService) RemoveUser(userName string) error {
	nsKV := e.getKV(userNS)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := nsKV.Delete(ctx, userName)
	cancel()

	return err
}

func (e *EtcdStorageService) RemoveUsers() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := e.client.Delete(ctx, userNS, clientv3.WithPrefix())
	cancel()

	return err
}

// GetUser get UserInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// UserInfo,  if there is no matching, return nil UserInfo
// End clients need check if returned UserInfo is nil before use when err is nil
func (e *EtcdStorageService) GetUser(userName string) (*UserInfo, error) {
	nsKV := e.getKV(userNS)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := nsKV.Get(ctx, userName)
	cancel()

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) > 0 {
		var user UserInfo
		err := json.Unmarshal(resp.Kvs[0].Value, &user)
		if err != nil {
			return nil, err
		}
		return &user, nil
	}

	// No match, return nil UserInfo and nil error
	return nil, nil
}

// GetUsers get all UserInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// UserInfo,  if there is no matching, return nil UserInfo
// End clients need check if returned UserInfo is nil before use when err is nil
func (e *EtcdStorageService) GetUsers() ([]*UserInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	resp, err := e.client.Get(ctx, userNS, clientv3.WithPrefix())
	cancel()

	if err != nil {
		return nil, err
	}

	var users []*UserInfo
	for _, kv := range resp.Kvs {
		var user UserInfo
		err := json.Unmarshal(kv.Value, &user)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}

	return users, nil
}

func (e *EtcdStorageService) UpdateUser(user *UserInfo) error {
	return e.AddUser(user)
}

func (e *EtcdStorageService) WatchUser(userName string) (clientv3.WatchChan, error) {
	ctx := context.Background()
	key := fmt.Sprintf("%s%s", userNS, userName)
	return e.watcher.Watch(ctx, key), nil
}

// Clients are reponsive for closing the returned WatchChan
func (e *EtcdStorageService) WatchUsers() (clientv3.WatchChan, error) {
	ctx := context.Background()
	return e.watcher.Watch(ctx, userNS, clientv3.WithPrefix()), nil
}

func (e *EtcdStorageService) Close() error {
	return e.client.Close()
}
