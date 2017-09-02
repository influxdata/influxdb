package meta

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"golang.org/x/net/context"
)

const (
	nsSep  = "/"
	rootNS = "/influxdb"
	userNS = rootNS + "/users"
	dbNS   = rootNS + "/dbs"
	rpNS   = rootNS + "/rps"
	sgNS   = rootNS + "/sgs"
	subNS  = rootNS + "/subs"
	cqNS   = rootNS + "/cqs"
	nodeNS = rootNS + "/nodes"

	master      = rootNS + "/master"
	masterEpoch = rootNS + "/master_epoch"
)

type EtcdStorageService struct {
	client  *clientv3.Client
	watcher clientv3.Watcher

	config *Config
}

func NewEtcdStorageService(config *Config) (*EtcdStorageService, error) {
	endpoints := strings.Split(config.EtcdEndpoints, ",")
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
		config:  config,
	}, nil
}

func (e *EtcdStorageService) Load() (*Data, error) {
	data := &Data{}

	// node
	nodes, err := e.GetNodes()
	if err != nil {
		return nil, err
	}
	data.Nodes = make(map[uint64]*NodeInfo, len(nodes))
	for _, node := range nodes {
		data.Nodes[node.ID] = node
	}

	// database
	dbs, err := e.GetDatabases()
	if err != nil {
		return nil, err
	}
	data.Databases = make(map[string]*DatabaseInfo, len(dbs))
	for _, db := range dbs {
		data.Databases[db.Name] = db
	}

	// user
	users, err := e.GetUsers()
	if err != nil {
		return nil, err
	}
	data.Users = make(map[string]*UserInfo, len(users))
	for _, u := range users {
		data.Users[u.Name] = u
	}

	// rp
	for name, db := range data.Databases {
		rps, err := e.GetRetentionPolicies(name)
		if err != nil {
			return nil, err
		}

		db.RetentionPolicies = make(map[string]*RetentionPolicyInfo, len(rps))
		for _, rp := range rps {
			db.RetentionPolicies[rp.Name] = rp
		}
	}

	// cq
	for name, db := range data.Databases {
		cqs, err := e.GetContinuousQueries(name)
		if err != nil {
			return nil, err
		}

		db.ContinuousQueries = make(map[string]*ContinuousQueryInfo, len(cqs))
		for _, cq := range cqs {
			db.ContinuousQueries[cq.Name] = cq
		}
	}

	// sg
	for dbName, db := range data.Databases {
		for rpName, rp := range db.RetentionPolicies {
			sgs, err := e.GetShardGroups(dbName, rpName)
			if err != nil {
				return nil, err
			}

			rp.ShardGroups = sgs
		}
	}

	// sub
	for dbName, db := range data.Databases {
		for rpName, rp := range db.RetentionPolicies {
			subs, err := e.GetSubscriptions(dbName, rpName)
			if err != nil {
				return nil, err
			}

			rp.Subscriptions = make(map[string]*SubscriptionInfo, len(subs))
			for _, sub := range subs {
				rp.Subscriptions[sub.Name] = sub
			}
		}
	}

	return data, nil
}

func (e *EtcdStorageService) AddUser(user *UserInfo) error {
	return e.addKeyValue(userNS, user.Name, user.marshal(), -1)
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
		var iuser internal.UserInfo
		err := proto.Unmarshal(data, &iuser)
		if err != nil {
			return nil, err
		}
		var user UserInfo
		user.unmarshal(&iuser)
		return &user, nil
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
		var iuser internal.UserInfo
		err := proto.Unmarshal(data, &iuser)
		if err != nil {
			return nil, err
		}

		var user UserInfo
		user.unmarshal(&iuser)
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
	return e.addKeyValue(dbNS, db.Name, db.marshal(), -1)
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
		var idb internal.DatabaseInfo
		err := proto.Unmarshal(data, &idb)
		if err != nil {
			return nil, err
		}

		var db DatabaseInfo
		db.unmarshal(&idb)
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
		var idb internal.DatabaseInfo
		err := proto.Unmarshal(data, &idb)
		if err != nil {
			return nil, err
		}

		var db DatabaseInfo
		db.unmarshal(&idb)
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

func (e *EtcdStorageService) AddContinuousQuery(dbName string, cq *ContinuousQueryInfo) error {
	return e.addKeyValue(getKey(cqNS, dbName), cq.Name, cq.marshal(), -1)
}

func (e *EtcdStorageService) DeleteContinuousQuery(dbName, cqName string) error {
	return e.deleteKeyValue(getKey(cqNS, dbName), cqName)
}

func (e *EtcdStorageService) DeleteContinuousQueries(dbName string) error {
	return e.deleteKeyValues(getNSKey(cqNS, dbName))
}

// GetContinuousQuery get ContinuousQueryInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// ContinuousQueryInfo,  if there is no matching, return nil ContinuousQueryInfo
// End clients need check if returned ContinuousQueryInfo is nil before use when err is nil
func (e *EtcdStorageService) GetContinuousQuery(dbName, cqName string) (*ContinuousQueryInfo, error) {
	data, err := e.getKeyValue(getKey(cqNS, dbName), cqName)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var icq internal.ContinuousQueryInfo
		err := proto.Unmarshal(data, &icq)
		if err != nil {
			return nil, err
		}
		var cq ContinuousQueryInfo
		cq.unmarshal(&icq)
		return &cq, nil
	}

	// No match, return nil ContinuousQueryInfo and nil error
	return nil, nil
}

// GetContinuousQueries get all ContinuousQueryInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// ContinuousQueryInfo,  if there is no matching, return nil ContinuousQueryInfo
// End clients need check if returned ContinuousQueryInfo is nil before use when err is nil
func (e *EtcdStorageService) GetContinuousQueries(dbName string) ([]*ContinuousQueryInfo, error) {
	results, err := e.getKeyValues(getNSKey(cqNS, dbName))
	if err != nil {
		return nil, err
	}

	var cqs []*ContinuousQueryInfo
	for _, data := range results {
		var icq internal.ContinuousQueryInfo
		err := proto.Unmarshal(data, &icq)
		if err != nil {
			return nil, err
		}
		var cq ContinuousQueryInfo
		cq.unmarshal(&icq)
		cqs = append(cqs, &cq)
	}

	return cqs, nil
}

func (e *EtcdStorageService) UpdateContinuousQuery(dbName string, cq *ContinuousQueryInfo) error {
	return e.AddContinuousQuery(dbName, cq)
}

func (e *EtcdStorageService) WatchContinuousQuery(dbName, cqName string) (clientv3.WatchChan, error) {
	return e.watchKey(getKey(cqNS, dbName), cqName)
}

func (e *EtcdStorageService) WatchContinuousQueries(dbName string) (clientv3.WatchChan, error) {
	return e.watchNS(getNSKey(cqNS, dbName))
}

func (e *EtcdStorageService) AddRetentionPolicy(dbName string, rp *RetentionPolicyInfo) error {
	return e.addKeyValue(getKey(rpNS, dbName), rp.Name, rp.marshal(), -1)
}

func (e *EtcdStorageService) DeleteRetentionPolicy(dbName, rpName string) error {
	return e.deleteKeyValue(getKey(rpNS, dbName), rpName)
}

func (e *EtcdStorageService) DeleteRetentionPolicies(dbName string) error {
	return e.deleteKeyValues(getNSKey(rpNS, dbName))
}

// GetRetentionPolicy get RetentionPolicyInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// RetentionPolicyInfo,  if there is no matching, return nil RetentionPolicyInfo
// End clients need check if returned RetentionPolicyInfo is nil before use when err is nil
func (e *EtcdStorageService) GetRetentionPolicy(dbName, rpName string) (*RetentionPolicyInfo, error) {
	data, err := e.getKeyValue(getKey(rpNS, dbName), rpName)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var irp internal.RetentionPolicyInfo
		err := proto.Unmarshal(data, &irp)
		if err != nil {
			return nil, err
		}
		var rp RetentionPolicyInfo
		rp.unmarshal(&irp)
		return &rp, nil
	}

	// No match, return nil RetentionPolicyInfo and nil error
	return nil, nil
}

// GetRetentionPolicies get all RetentionPolicyInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// RetentionPolicyInfo,  if there is no matching, return nil RetentionPolicyInfo
// End clients need check if returned RetentionPolicyInfo is nil before use when err is nil
func (e *EtcdStorageService) GetRetentionPolicies(dbName string) ([]*RetentionPolicyInfo, error) {
	results, err := e.getKeyValues(getNSKey(rpNS, dbName))
	if err != nil {
		return nil, err
	}

	var rps []*RetentionPolicyInfo
	for _, data := range results {
		var irp internal.RetentionPolicyInfo
		err := proto.Unmarshal(data, &irp)
		if err != nil {
			return nil, err
		}
		var rp RetentionPolicyInfo
		rp.unmarshal(&irp)
		rps = append(rps, &rp)
	}

	return rps, nil
}

func (e *EtcdStorageService) UpdateRetentionPolicy(dbName string, rp *RetentionPolicyInfo) error {
	return e.AddRetentionPolicy(dbName, rp)
}

func (e *EtcdStorageService) WatchRetentionPolicy(dbName, rpName string) (clientv3.WatchChan, error) {
	return e.watchKey(getKey(rpNS, dbName), rpName)
}

func (e *EtcdStorageService) WatchRetentionPolicies(dbName string) (clientv3.WatchChan, error) {
	return e.watchNS(getNSKey(rpNS, dbName))
}

func (e *EtcdStorageService) AddShardGroup(dbName, rpName string, sg *ShardGroupInfo) error {
	return e.addKeyValue(getKey(sgNS, dbName, rpName), fmt.Sprintf("%d", sg.ID), sg.marshal(), -1)
}

func (e *EtcdStorageService) DeleteShardGroup(dbName, rpName, sgID string) error {
	return e.deleteKeyValue(getKey(sgNS, dbName, rpName), sgID)
}

func (e *EtcdStorageService) DeleteShardGroups(dbName, rpName string) error {
	return e.deleteKeyValues(getNSKey(sgNS, dbName, rpName))
}

// GetShardGroup get ShardGroupInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// ShardGroupInfo,  if there is no matching, return nil ShardGroupInfo
// End clients need check if returned ShardGroupInfo is nil before use when err is nil
func (e *EtcdStorageService) GetShardGroup(dbName, rpName, sgID string) (*ShardGroupInfo, error) {
	data, err := e.getKeyValue(getKey(sgNS, dbName, rpName), sgID)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var isg internal.ShardGroupInfo
		err := proto.Unmarshal(data, &isg)
		if err != nil {
			return nil, err
		}
		var sg ShardGroupInfo
		sg.unmarshal(&isg)
		return &sg, nil
	}

	// No match, return nil ShardGroupInfo and nil error
	return nil, nil
}

// GetRetentionPolicies get all ShardGroupInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// ShardGroupInfo,  if there is no matching, return nil ShardGroupInfo
// End clients need check if returned ShardGroupInfo is nil before use when err is nil
func (e *EtcdStorageService) GetShardGroups(dbName, rpName string) ([]*ShardGroupInfo, error) {
	results, err := e.getKeyValues(getNSKey(sgNS, dbName, rpName))
	if err != nil {
		return nil, err
	}

	var sgs []*ShardGroupInfo
	for _, data := range results {
		var isg internal.ShardGroupInfo
		err := proto.Unmarshal(data, &isg)
		if err != nil {
			return nil, err
		}
		var sg ShardGroupInfo
		sg.unmarshal(&isg)
		sgs = append(sgs, &sg)
	}

	return sgs, nil
}

func (e *EtcdStorageService) UpdateShardGroup(dbName, rpName string, sg *ShardGroupInfo) error {
	return e.AddShardGroup(dbName, rpName, sg)
}

func (e *EtcdStorageService) WatchShardGroup(dbName, rpName, sgID string) (clientv3.WatchChan, error) {
	return e.watchKey(getKey(sgNS, dbName, rpName), sgID)
}

func (e *EtcdStorageService) WatchShardGroups(dbName, rpName string) (clientv3.WatchChan, error) {
	return e.watchNS(getNSKey(sgNS, dbName, rpName))
}

func (e *EtcdStorageService) AddSubscription(dbName, rpName string, sub *SubscriptionInfo) error {
	return e.addKeyValue(getKey(subNS, dbName, rpName), sub.Name, sub.marshal(), -1)
}

func (e *EtcdStorageService) DeleteSubscription(dbName, rpName, subName string) error {
	return e.deleteKeyValue(getKey(subNS, dbName, rpName), subName)
}

func (e *EtcdStorageService) DeleteSubscriptions(dbName, rpName string) error {
	return e.deleteKeyValues(getNSKey(subNS, dbName, rpName))
}

// GetSubscription get SubscriptionInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// SubscriptionInfo,  if there is no matching, return nil SubscriptionInfo
// End clients need check if returned SubscriptionInfo is nil before use when err is nil
func (e *EtcdStorageService) GetSubscription(dbName, rpName, subName string) (*SubscriptionInfo, error) {
	data, err := e.getKeyValue(getKey(subNS, dbName, rpName), subName)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var isub internal.SubscriptionInfo
		err := proto.Unmarshal(data, &isub)
		if err != nil {
			return nil, err
		}
		var sub SubscriptionInfo
		sub.unmarshal(&isub)
		return &sub, nil
	}

	// No match, return nil SubscriptionInfo and nil error
	return nil, nil
}

// GetRetentionPolicies get all SubscriptionInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// SubscriptionInfo,  if there is no matching, return nil SubscriptionInfo
// End clients need check if returned SubscriptionInfo is nil before use when err is nil
func (e *EtcdStorageService) GetSubscriptions(dbName, rpName string) ([]*SubscriptionInfo, error) {
	results, err := e.getKeyValues(getNSKey(subNS, dbName, rpName))
	if err != nil {
		return nil, err
	}

	var subs []*SubscriptionInfo
	for _, data := range results {
		var isub internal.SubscriptionInfo
		err := proto.Unmarshal(data, &isub)
		if err != nil {
			return nil, err
		}
		var sub SubscriptionInfo
		sub.unmarshal(&isub)
		subs = append(subs, &sub)
	}

	return subs, nil
}

func (e *EtcdStorageService) UpdateSubscription(dbName, rpName string, sub *SubscriptionInfo) error {
	return e.AddSubscription(dbName, rpName, sub)
}

func (e *EtcdStorageService) WatchSubscription(dbName, rpName, subName string) (clientv3.WatchChan, error) {
	return e.watchKey(getKey(subNS, dbName, rpName), subName)
}

func (e *EtcdStorageService) WatchSubscriptions(dbName, rpName string) (clientv3.WatchChan, error) {
	return e.watchNS(getNSKey(subNS, dbName, rpName))
}

func (e *EtcdStorageService) AddNode(node *NodeInfo) error {
	return e.addKeyValue(nodeNS, fmt.Sprintf("%d", node.ID), node.marshal(), e.config.LeaseDuration)
}

func (e *EtcdStorageService) DeleteNode(nodeID string) error {
	return e.deleteKeyValue(nodeNS, nodeID)
}

func (e *EtcdStorageService) DeleteNodes() error {
	return e.deleteKeyValues(nodeNS)
}

// GetNode get NodeInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// NodeInfo,  if there is no matching, return nil NodeInfo
// End clients need check if returned NodeInfo is nil before use when err is nil
func (e *EtcdStorageService) GetNode(nodeID string) (*NodeInfo, error) {
	data, err := e.getKeyValue(nodeNS, nodeID)
	if err != nil {
		return nil, err
	}

	if data != nil {
		var inode internal.NodeInfo
		err := proto.Unmarshal(data, &inode)
		if err != nil {
			return nil, err
		}
		var node NodeInfo
		node.unmarshal(&inode)
		return &node, nil
	}

	// No match, return nil NodeInfo and nil error
	return nil, nil
}

// GetNodes get all NodeInfo from etcd. If there is no error happened during the
// query, return error directly. Otherwise if there is matching, returns non-nil
// NodeInfo,  if there is no matching, return nil NodeInfo
// End clients need check if returned NodeInfo is nil before use when err is nil
func (e *EtcdStorageService) GetNodes() ([]*NodeInfo, error) {
	results, err := e.getKeyValues(nodeNS)
	if err != nil {
		return nil, err
	}

	var nodes []*NodeInfo
	for _, data := range results {
		var inode internal.NodeInfo
		err := proto.Unmarshal(data, &inode)
		if err != nil {
			return nil, err
		}
		var node NodeInfo
		node.unmarshal(&inode)
		nodes = append(nodes, &node)
	}

	return nodes, nil
}

func (e *EtcdStorageService) UpdateNode(db *NodeInfo) error {
	return e.AddNode(db)
}

func (e *EtcdStorageService) WatchNode(nodeID string) (clientv3.WatchChan, error) {
	return e.watchKey(nodeNS, nodeID)
}

func (e *EtcdStorageService) WatchNodes() (clientv3.WatchChan, error) {
	return e.watchNS(nodeNS)
}

func (e *EtcdStorageService) Close() error {
	err := e.watcher.Close()
	if err != nil {
		return err
	}

	return e.client.Close()
}

func (e *EtcdStorageService) addKeyValue(ns, k string, v proto.Message, ttl int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	data, err := proto.Marshal(v)
	if err != nil {
		return err
	}

	var opts []clientv3.OpOption
	if ttl > 0 {
		resp, err := e.client.Grant(context.TODO(), ttl)
		if err != nil {
			return err
		}

		opts = append(opts, clientv3.WithLease(resp.ID))
		// FIXME, ignore the result chan, problem ?
		if _, err := e.client.KeepAlive(context.TODO(), resp.ID); err != nil {
			return err
		}
	}

	_, err = e.client.Put(ctx, getKey(ns, k), string(data), opts...)
	cancel()

	return err
}

func (e *EtcdStorageService) deleteKeyValue(ns, k string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := e.client.Delete(ctx, getKey(ns, k))
	cancel()

	return err
}

func (e *EtcdStorageService) deleteKeyValues(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := e.client.Delete(ctx, getKey(ns, ""), clientv3.WithPrefix())
	cancel()

	return err
}

func (e *EtcdStorageService) getKeyValue(ns, k string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := e.client.Get(ctx, getKey(ns, k))
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
	resp, err := e.client.Get(ctx, getKey(ns, ""), clientv3.WithPrefix())
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
	return e.watcher.Watch(ctx, getKey(ns, key)), nil
}

func (e *EtcdStorageService) watchNS(ns string) (clientv3.WatchChan, error) {
	ctx := context.Background()
	return e.watcher.Watch(ctx, ns, clientv3.WithPrefix()), nil
}

func getKey(segments ...string) string {
	return strings.Join(segments, nsSep)
}

func getNSKey(ns string, segments ...string) string {
	nonEmpties := []string{ns}
	for _, k := range segments {
		if k != "" {
			nonEmpties = append(nonEmpties, k)
		}
	}

	if len(nonEmpties) > 1 {
		return getKey(nonEmpties...)
	}
	return ns
}
