package meta

import (
	"encoding/json"
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

	wlock      sync.RWMutex
	nsWatchers map[string]clientv3.Watcher
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
		client:     client,
		watcher:    clientv3.NewWatcher(client),
		nsKVs:      make(map[string]clientv3.KV, 6),
		nsWatchers: make(map[string]clientv3.Watcher, 6),
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

// getWatcher returns namespace Watcher according to namespace passed in
// if the corresponding namespace Watcher doesn't exist, create it
func (e *EtcdStorageService) getWatcher(ns string) clientv3.Watcher {
	e.wlock.RLock()
	if nsWatcher, ok := e.nsWatchers[ns]; ok {
		e.wlock.RUnlock()
		return nsWatcher
	}

	// Release read lock
	e.wlock.RUnlock()

	// Hold write lock
	e.wlock.Lock()

	// Re-evaluate
	if _, ok := e.nsWatchers[ns]; !ok {
		e.nsWatchers[ns] = namespace.NewWatcher(e.watcher, ns)
	}

	nsWatcher := e.nsWatchers[ns]
	e.lock.Unlock()

	return nsWatcher
}

func (e *EtcdStorageService) AddUser(user *UserInfo) error {
	return e.addKeyValue(userNS, user.Name, user)
}

func (e *EtcdStorageService) DeleteUser(userName string) error {
	return e.deleteKeyValue(userNS, userName)
}

func (e *EtcdStorageService) DeleteUsers() error {
	return e.deleteKeyValues(userNS)
}

// GetUsers get UserInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// UserInfo,  if there is no matching, return nil UserInfo
// End clients need check if returned UserInfo is nil before use when err is nil
func (e *EtcdStorageService) GetUser(userName string) (*UserInfo, error) {
	data, err := e.getKeyValue(userNS, userName)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var u UserInfo
		err := json.Unmarshal(data, &u)
		if err != nil {
			return nil, err
		}
		return &u, nil
	}
	return nil, nil
}

// GetUsers get all UserInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// UserInfo,  if there is no matching, return nil UserInfo
// End clients need check if returned UserInfo is nil before use when err is nil
func (e *EtcdStorageService) GetUsers() ([]*UserInfo, error) {
	results, err := e.getKeyValues(userNS)
	if err != nil {
		return nil, err
	}

	var users []*UserInfo
	for _, data := range results {
		var user UserInfo
		err := json.Unmarshal(data, &user)
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
	return e.watchKey(userNS, userName)
}

func (e *EtcdStorageService) WatchUsers() (clientv3.WatchChan, error) {
	return e.watchNS(userNS)
}

func (e *EtcdStorageService) AddDatabase(db *DatabaseInfo) error {
	return e.addKeyValue(dbNS, db.Name, db)
}

func (e *EtcdStorageService) DeleteDatabase(dbName string) error {
	return e.deleteKeyValue(dbNS, dbName)
}

func (e *EtcdStorageService) DeleteDatabases() error {
	return e.deleteKeyValues(dbNS)
}

// GetDatabase get DatabaseInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// DatabaseInfo,  if there is no matching, return nil DatabaseInfo
// End clients need check if returned DatabaseInfo is nil before use when err is nil
func (e *EtcdStorageService) GetDatabase(dbName string) (*DatabaseInfo, error) {
	data, err := e.getKeyValue(dbNS, dbName)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var db DatabaseInfo
		err := json.Unmarshal(data, &db)
		if err != nil {
			return nil, err
		}
		return &db, nil
	}

	// No match, return nil DatabaseInfo and nil error
	return nil, nil
}

// GetDatabases get all DatabaseInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// DatabaseInfo,  if there is no matching, return nil DatabaseInfo
// End clients need check if returned DatabaseInfo is nil before use when err is nil
func (e *EtcdStorageService) GetDatabases() ([]*DatabaseInfo, error) {
	results, err := e.getKeyValues(dbNS)
	if err != nil {
		return nil, err
	}

	var dbs []*DatabaseInfo
	for _, data := range results {
		var db DatabaseInfo
		err := json.Unmarshal(data, &db)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, &db)
	}

	return dbs, nil
}

func (e *EtcdStorageService) UpdateDatabase(db *DatabaseInfo) error {
	return e.AddDatabase(db)
}

func (e *EtcdStorageService) WatchDatabase(dbName string) (clientv3.WatchChan, error) {
	return e.watchKey(dbNS, dbName)
}

func (e *EtcdStorageService) WatchDatabases() (clientv3.WatchChan, error) {
	return e.watchNS(dbNS)
}

func (e *EtcdStorageService) Close() error {
	err := e.watcher.Close()
	if err != nil {
		return err
	}

	e.wlock.Lock()
	defer e.wlock.Unlock()

	for _, w := range e.nsWatchers {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	return e.client.Close()
}

func (e *EtcdStorageService) addKeyValue(ns, k string, v interface{}) error {
	nsKV := e.getKV(ns)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = nsKV.Put(ctx, k, string(data))
	cancel()

	return err
}

func (e *EtcdStorageService) deleteKeyValue(ns, k string) error {
	nsKV := e.getKV(ns)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := nsKV.Delete(ctx, k)
	cancel()

	return err
}

func (e *EtcdStorageService) deleteKeyValues(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := e.client.Delete(ctx, ns, clientv3.WithPrefix())
	cancel()

	return err
}

func (e *EtcdStorageService) getKeyValue(ns, k string) ([]byte, error) {
	nsKV := e.getKV(ns)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := nsKV.Get(ctx, k)
	cancel()

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) > 0 {
		return resp.Kvs[0].Value, nil
	}

	// No match, return not found and nil error
	return nil, nil
}

func (e *EtcdStorageService) getKeyValues(ns string) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	resp, err := e.client.Get(ctx, ns, clientv3.WithPrefix())
	cancel()

	if err != nil {
		return nil, err
	}

	var results [][]byte
	for _, kv := range resp.Kvs {
		results = append(results, kv.Value)
	}
	return results, nil
}

func (e *EtcdStorageService) watchKey(ns, key string) (clientv3.WatchChan, error) {
	ctx := context.Background()
	watcher := e.getWatcher(ns)
	return watcher.Watch(ctx, key), nil
}

func (e *EtcdStorageService) watchNS(ns string) (clientv3.WatchChan, error) {
	ctx := context.Background()
	return e.watcher.Watch(ctx, ns, clientv3.WithPrefix()), nil
}
