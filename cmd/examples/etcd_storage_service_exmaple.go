package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"gopkg.in/alecthomas/kingpin.v2"
)

type EtcdStorageServiceExample struct {
	ess        *meta.EtcdStorageService
	cmd        string
	objectType string
	key        string
	all        bool

	// Used to create rp and shard group
	database string

	// Used to create shard group
	retentionPolicy string
}

func (e *EtcdStorageServiceExample) deleteMaster() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) deleteMasterEpoch() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) handleDelete() (interface{}, error) {
	deleteHandlers := map[string]func() (interface{}, error){
		"user":          e.deleteUser,
		"db":            e.deleteDatabase,
		"node":          e.deleteNode,
		"rp":            e.deleteRetentionPolicy,
		"master":        e.deleteMaster,
		"master_epoche": e.deleteMasterEpoch,
	}

	return deleteHandlers[e.objectType]()
}

func (e *EtcdStorageServiceExample) addMaster() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) addMasterEpoch() (interface{}, error) {
	return nil, nil
}

// Node
func (e *EtcdStorageServiceExample) watchNode() (interface{}, error) {
	var err error
	var ch clientv3.WatchChan

	if e.all {
		ch, err = e.ess.WatchNodes()
	} else {
		ch, err = e.ess.WatchNode(e.key)
	}

	if err != nil {
		return nil, err
	}

	for {
		res := <-ch
		data, _ := json.Marshal(res)
		fmt.Printf("%s\n", data)
	}
	return nil, nil
}

func (e *EtcdStorageServiceExample) getNode() (interface{}, error) {
	if e.all {
		return e.ess.GetNodes()
	}
	return e.ess.GetNode(e.key)
}

func (e *EtcdStorageServiceExample) deleteNode() (interface{}, error) {
	if e.all {
		return nil, e.ess.DeleteNodes()
	}
	return nil, e.ess.DeleteNode(e.key)
}

func (e *EtcdStorageServiceExample) addNode() (interface{}, error) {
	node := &meta.NodeInfo{
		ID:      uint64(time.Now().UnixNano()),
		Host:    "localhost",
		TCPHost: "localhost",
	}
	err := e.ess.AddNode(node)
	if err != nil {
		return nil, err
	}
	return node, err
}

// Database
func (e *EtcdStorageServiceExample) watchDatabase() (interface{}, error) {
	var err error
	var ch clientv3.WatchChan

	if e.all {
		ch, err = e.ess.WatchDatabases()
	} else {
		ch, err = e.ess.WatchDatabase(e.key)
	}

	if err != nil {
		return nil, err
	}

	for {
		res := <-ch
		data, _ := json.Marshal(res)
		fmt.Printf("%s\n", data)
	}
	return nil, nil
}

func (e *EtcdStorageServiceExample) getDatabase() (interface{}, error) {
	if e.all {
		return e.ess.GetDatabases()
	}
	return e.ess.GetDatabase(e.key)
}

func (e *EtcdStorageServiceExample) deleteDatabase() (interface{}, error) {
	if e.all {
		return nil, e.ess.DeleteDatabases()
	}
	return nil, e.ess.DeleteDatabase(e.key)
}

func (e *EtcdStorageServiceExample) addDatabase() (interface{}, error) {
	db := &meta.DatabaseInfo{
		Name: fmt.Sprintf("db_%d", time.Now().UnixNano()),
		DefaultRetentionPolicy: "rpt_7",
	}
	err := e.ess.AddDatabase(db)
	if err != nil {
		return nil, err
	}
	return db, err
}

// RententionPolicy
func (e *EtcdStorageServiceExample) watchRetentionPolicy() (interface{}, error) {
	if e.database == "" {
		return nil, errors.New("database should be specified")
	}

	var err error
	var ch clientv3.WatchChan

	if e.all {
		ch, err = e.ess.WatchRetentionPolicies(e.database)
	} else {
		ch, err = e.ess.WatchRetentionPolicy(e.database, e.key)
	}

	if err != nil {
		return nil, err
	}

	for {
		res := <-ch
		data, _ := json.Marshal(res)
		fmt.Printf("%s\n", data)
	}
	return nil, nil
}

func (e *EtcdStorageServiceExample) getRetentionPolicy() (interface{}, error) {
	if e.database == "" {
		return nil, errors.New("database should be specified")
	}

	if e.all {
		return e.ess.GetRetentionPolicies(e.database)
	}
	return e.ess.GetRetentionPolicy(e.database, e.key)
}

func (e *EtcdStorageServiceExample) deleteRetentionPolicy() (interface{}, error) {
	if e.database == "" {
		return nil, errors.New("database should be specified")
	}

	if e.all {
		return nil, e.ess.DeleteRetentionPolicies(e.database)
	}
	return nil, e.ess.DeleteRetentionPolicy(e.database, e.key)
}

func (e *EtcdStorageServiceExample) addRetentionPolicy() (interface{}, error) {
	if e.database == "" {
		return nil, errors.New("database should be specified")
	}

	rp := &meta.RetentionPolicyInfo{
		Name:               fmt.Sprintf("rp_%d", time.Now().UnixNano()),
		ReplicaN:           3,
		Duration:           time.Duration(86400),
		ShardGroupDuration: time.Duration(7 * 86400),
	}

	err := e.ess.AddRetentionPolicy(e.database, rp)
	if err != nil {
		return nil, err
	}
	return rp, err
}

// User
func (e *EtcdStorageServiceExample) watchUser() (interface{}, error) {
	var err error
	var ch clientv3.WatchChan

	if e.all {
		ch, err = e.ess.WatchUsers()
	} else {
		ch, err = e.ess.WatchUser(e.key)
	}

	if err != nil {
		return nil, err
	}

	for {
		res := <-ch
		data, _ := json.Marshal(res)
		fmt.Printf("%s\n", data)
	}
	return nil, nil
}

func (e *EtcdStorageServiceExample) getUser() (interface{}, error) {
	if e.all {
		return e.ess.GetUsers()
	}
	return e.ess.GetUser(e.key)
}

func (e *EtcdStorageServiceExample) deleteUser() (interface{}, error) {
	if e.all {
		return nil, e.ess.DeleteUsers()
	}
	return nil, e.ess.DeleteUser(e.key)
}

func (e *EtcdStorageServiceExample) addUser() (interface{}, error) {
	user := meta.UserInfo{
		Name:  fmt.Sprintf("%s_%d", e.objectType, time.Now().UnixNano()),
		Hash:  fmt.Sprintf("I love you at %d", time.Now().UnixNano()),
		Admin: true,
		Privileges: map[string]influxql.Privilege{
			"snaproad": influxql.AllPrivileges,
		},
	}
	err := e.ess.AddUser(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (e *EtcdStorageServiceExample) handlePut() (interface{}, error) {
	putHandlers := map[string]func() (interface{}, error){
		"user":          e.addUser,
		"db":            e.addDatabase,
		"node":          e.addNode,
		"rp":            e.addRetentionPolicy,
		"master":        e.addMaster,
		"master_epoche": e.addMasterEpoch,
	}

	return putHandlers[e.objectType]()
}

func (e *EtcdStorageServiceExample) getMaster() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) getMasterEpoch() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) handleGet() (interface{}, error) {
	getHandlers := map[string]func() (interface{}, error){
		"user":          e.getUser,
		"db":            e.getDatabase,
		"node":          e.getNode,
		"rp":            e.getRetentionPolicy,
		"master":        e.getMaster,
		"master_epoche": e.getMasterEpoch,
	}

	return getHandlers[e.objectType]()
}

func (e *EtcdStorageServiceExample) watchMaster() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) watchMasterEpoch() (interface{}, error) {
	return nil, nil
}

func (e *EtcdStorageServiceExample) handleWatch() (interface{}, error) {
	watchHandlers := map[string]func() (interface{}, error){
		"user":          e.watchUser,
		"db":            e.watchDatabase,
		"node":          e.watchNode,
		"master":        e.watchMaster,
		"master_epoche": e.watchMasterEpoch,
	}

	return watchHandlers[e.objectType]()
}

func (e *EtcdStorageServiceExample) Execute() (interface{}, error) {
	cmdHandlers := map[string]func() (interface{}, error){
		"put":   e.handlePut,
		"get":   e.handleGet,
		"del":   e.handleDelete,
		"watch": e.handleWatch,
	}

	return cmdHandlers[e.cmd]()
}

func (e *EtcdStorageServiceExample) Close() error {
	return e.ess.Close()
}

var (
	etcdEndpoints = kingpin.Flag("etcd-endpoints", "Etcd server endpoints").Default("localhost:2379").String()

	objectTypes   = []string{"db", "user", "node", "rp", "sg", "master", "master_epoche", "all"}
	objectType = kingpin.Flag("object-type", "Object to manipulate").Required().Enum(objectTypes...)

	// The following 2 flags are only used for creating rp and shard group
	rpDB = kingpin.Flag("rp-db", "Database which the rp belongs to").String()
	sgRP = kingpin.Flag("shard-sg-rp", "rp the shard group belongs to").String()

	putCmd        = kingpin.Command("put", "Create a new key/vaule")

	delCmd        = kingpin.Command("del", "Delete an existing key/vaule")
	delObjectKey  = delCmd.Flag("key", "The key of a object").String()
	delAll        = delCmd.Flag("all", "Delete all objects matched").Default("false").Bool()

	getCmd         = kingpin.Command("get", "Get an existing key/vaule")
	getObjectKey   = getCmd.Flag("key", "The key of a object").String()
	getAll         = getCmd.Flag("all", "Get all objects matched").Default("false").Bool()

	watchCmd        = kingpin.Command("watch", "Watch changes to keys")
	watchObjectKey  = watchCmd.Flag("key", "The key of a object").String()
	watchAll        = watchCmd.Flag("all", "Watch all objects matched").Default("false").Bool()
)

func main() {
	cmd := kingpin.Parse()
	config := meta.NewConfig()
	config.EtcdEndpoints = *etcdEndpoints

	e, err := meta.NewEtcdStorageService(config)
	if err != nil {
		panic(err)
	}

	key := map[string]string{
		"get":   *getObjectKey,
		"del":   *delObjectKey,
		"watch": *watchObjectKey,
	}[cmd]

	all := map[string]bool{
		"get":   *getAll,
		"del":   *delAll,
		"watch": *watchAll,
	}[cmd]

	esse := &EtcdStorageServiceExample{
		ess:             e,
		cmd:             cmd,
		objectType:      *objectType,
		key:             key,
		database:        *rpDB,
		retentionPolicy: *sgRP,
		all:             all,
	}

	res, err := esse.Execute()
	data, _ := json.Marshal(res)
	fmt.Printf("result=%s, err=%#v\n", data, err)
	esse.Close()
}

func TestUser(e *meta.EtcdStorageService) {
	user := meta.UserInfo{
		Name:  "user mike",
		Hash:  "I love you",
		Admin: true,
		Privileges: map[string]influxql.Privilege{
			"snaproad": influxql.AllPrivileges,
		},
	}
	err := e.AddUser(&user)
	if err != nil {
		panic(err)
	}

	u, err := e.GetUser(user.Name)
	if err != nil {
		panic(err)
	}

	assertEqual(u, &user)

	user.Hash = "i love you too much"
	err = e.UpdateUser(&user)
	if err != nil {
		panic(err)
	}

	u, err = e.GetUser(user.Name)
	if err != nil {
		panic(err)
	}

	assertEqual(u, &user)

	err = e.DeleteUser(user.Name)
	if err != nil {
		panic(err)
	}

	u, err = e.GetUser(user.Name)
	if err != nil {
		panic(err)
	}

	if u != nil {
		panic("expect nil")
	}
}

func assertEqual(lhs *meta.UserInfo, rhs *meta.UserInfo) {
	if lhs == nil && rhs != nil || lhs != nil && rhs == nil {
		panic("one nil and the other non-nil")
	}

	if lhs.Name != rhs.Name {
		panic(fmt.Errorf("name %s != %s", lhs.Name, rhs.Name))
	}

	if lhs.Hash != rhs.Hash {
		panic(fmt.Errorf("hash %s != %s", lhs.Hash, rhs.Hash))
	}

	if lhs.Admin != rhs.Admin {
		panic(fmt.Errorf("admin %+v != %+v", lhs.Admin, rhs.Admin))
	}
}
