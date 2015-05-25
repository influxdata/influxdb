package meta_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/meta"
)

// Ensure the store can be opened and closed.
func TestStore_Open(t *testing.T) {
	path := MustTempFile()
	defer os.RemoveAll(path)

	// Open store in temporary directory.
	s := NewStore()
	if err := s.Open(path); err != nil {
		t.Fatal(err)
	}
	defer s.Close() // idempotent

	// Wait for leadership change.
	select {
	case <-s.LeaderCh():
	case <-time.After(1 * time.Second):
		t.Fatal("no leadership")
	}

	time.Sleep(100 * time.Millisecond)

	// Close store.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure the store returns an error
func TestStore_Open_ErrStoreOpen(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()

	if err := s.Open(s.Path()); err != meta.ErrStoreOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the store can create a new node.
func TestStore_CreateNode(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create node.
	if ni, err := s.CreateNode("host0"); err != nil {
		t.Fatal(err)
	} else if *ni != (meta.NodeInfo{ID: 1, Host: "host0"}) {
		t.Fatalf("unexpected node: %#v", ni)
	}

	// Create another node.
	if ni, err := s.CreateNode("host1"); err != nil {
		t.Fatal(err)
	} else if *ni != (meta.NodeInfo{ID: 2, Host: "host1"}) {
		t.Fatalf("unexpected node: %#v", ni)
	}
}

// Ensure that creating an existing node returns an error.
func TestStore_CreateNode_ErrNodeExists(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create node.
	if _, err := s.CreateNode("host0"); err != nil {
		t.Fatal(err)
	}

	// Create it again.
	if _, err := s.CreateNode("host0"); err != meta.ErrNodeExists {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the store can find a node by ID.
func TestStore_Node(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create nodes.
	for i := 0; i < 3; i++ {
		if _, err := s.CreateNode(fmt.Sprintf("host%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Find second node.
	if ni, err := s.Node(2); err != nil {
		t.Fatal(err)
	} else if *ni != (meta.NodeInfo{ID: 2, Host: "host1"}) {
		t.Fatalf("unexpected node: %#v", ni)
	}
}

// Ensure the store can find a node by host.
func TestStore_NodeByHost(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create nodes.
	for i := 0; i < 3; i++ {
		if _, err := s.CreateNode(fmt.Sprintf("host%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Find second node.
	if ni, err := s.NodeByHost("host1"); err != nil {
		t.Fatal(err)
	} else if *ni != (meta.NodeInfo{ID: 2, Host: "host1"}) {
		t.Fatalf("unexpected node: %#v", ni)
	}
}

// Ensure the store can delete an existing node.
func TestStore_DeleteNode(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create nodes.
	for i := 0; i < 3; i++ {
		if _, err := s.CreateNode(fmt.Sprintf("host%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Remove second node.
	if err := s.DeleteNode(2); err != nil {
		t.Fatal(err)
	}

	// Ensure remaining nodes are correct.
	if ni, _ := s.Node(1); *ni != (meta.NodeInfo{ID: 1, Host: "host0"}) {
		t.Fatalf("unexpected node(1): %#v", ni)
	}
	if ni, _ := s.Node(2); ni != nil {
		t.Fatalf("unexpected node(2): %#v", ni)
	}
	if ni, _ := s.Node(3); *ni != (meta.NodeInfo{ID: 3, Host: "host2"}) {
		t.Fatalf("unexpected node(3): %#v", ni)
	}
}

// Ensure the store returns an error when deleting a node that doesn't exist.
func TestStore_DeleteNode_ErrNodeNotFound(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	if err := s.DeleteNode(2); err != meta.ErrNodeNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the store can create a new database.
func TestStore_CreateDatabase(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create database.
	if di, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(di, &meta.DatabaseInfo{Name: "db0"}) {
		t.Fatalf("unexpected database: %#v", di)
	}

	// Create another database.
	if di, err := s.CreateDatabase("db1"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(di, &meta.DatabaseInfo{Name: "db1"}) {
		t.Fatalf("unexpected database: %#v", di)
	}
}

// Ensure the store can delete an existing database.
func TestStore_DropDatabase(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create databases.
	for i := 0; i < 3; i++ {
		if _, err := s.CreateDatabase(fmt.Sprintf("db%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Remove a database.
	if err := s.DropDatabase("db1"); err != nil {
		t.Fatal(err)
	}

	// Ensure remaining nodes are correct.
	if di, _ := s.Database("db0"); !reflect.DeepEqual(di, &meta.DatabaseInfo{Name: "db0"}) {
		t.Fatalf("unexpected database(0): %#v", di)
	}
	if di, _ := s.Database("db1"); di != nil {
		t.Fatalf("unexpected database(1): %#v", di)
	}
	if di, _ := s.Database("db2"); !reflect.DeepEqual(di, &meta.DatabaseInfo{Name: "db2"}) {
		t.Fatalf("unexpected database(2): %#v", di)
	}
}

// Ensure the store returns an error when dropping a database that doesn't exist.
func TestStore_DropDatabase_ErrDatabaseNotFound(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	if err := s.DropDatabase("no_such_database"); err != meta.ErrDatabaseNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the store can create a retention policy on a database.
func TestStore_CreateRetentionPolicy(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Create policy on database.
	if rpi, err := s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{
		Name:     "rp0",
		ReplicaN: 2,
		Duration: 48 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(rpi, &meta.RetentionPolicyInfo{
		Name:               "rp0",
		ReplicaN:           2,
		Duration:           48 * time.Hour,
		ShardGroupDuration: 24 * time.Hour,
	}) {
		t.Fatalf("unexpected policy: %#v", rpi)
	}
}

// Ensure the store can delete a retention policy.
func TestStore_DropRetentionPolicy(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Create policies.
	for i := 0; i < 3; i++ {
		if _, err := s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: fmt.Sprintf("rp%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	// Remove a policy.
	if err := s.DropRetentionPolicy("db0", "rp1"); err != nil {
		t.Fatal(err)
	}

	// Ensure remaining policies are correct.
	if rpi, _ := s.RetentionPolicy("db0", "rp0"); !reflect.DeepEqual(rpi, &meta.RetentionPolicyInfo{Name: "rp0", ShardGroupDuration: 7 * 24 * time.Hour}) {
		t.Fatalf("unexpected policy(0): %#v", rpi)
	}
	if rpi, _ := s.RetentionPolicy("db0", "rp1"); rpi != nil {
		t.Fatalf("unexpected policy(1): %#v", rpi)
	}
	if rpi, _ := s.RetentionPolicy("db0", "rp2"); !reflect.DeepEqual(rpi, &meta.RetentionPolicyInfo{Name: "rp2", ShardGroupDuration: 7 * 24 * time.Hour}) {
		t.Fatalf("unexpected policy(2): %#v", rpi)
	}
}

// Ensure the store can set the default retention policy on a database.
func TestStore_SetDefaultRetentionPolicy(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0"}); err != nil {
		t.Fatal(err)
	}

	// Set default policy.
	if err := s.SetDefaultRetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	}

	// Ensure default policy is set.
	if di, _ := s.Database("db0"); di.DefaultRetentionPolicy != "rp0" {
		t.Fatalf("unexpected default retention policy: %s", di.DefaultRetentionPolicy)
	}
}

// Ensure the store can update a retention policy.
func TestStore_UpdateRetentionPolicy(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0"}); err != nil {
		t.Fatal(err)
	}

	// Update policy.
	var rpu meta.RetentionPolicyUpdate
	rpu.SetName("rp1")
	rpu.SetDuration(10 * time.Hour)
	rpu.SetReplicaN(3)
	if err := s.UpdateRetentionPolicy("db0", "rp0", &rpu); err != nil {
		t.Fatal(err)
	}

	// Ensure policy is updated.
	if rpi, err := s.RetentionPolicy("db0", "rp1"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(rpi, &meta.RetentionPolicyInfo{
		Name:               "rp1",
		Duration:           10 * time.Hour,
		ShardGroupDuration: 7 * 24 * time.Hour,
		ReplicaN:           3,
	}) {
		t.Fatalf("unexpected policy: %#v", rpi)
	}
}

// Ensure the store can create a shard group on a retention policy.
func TestStore_CreateShardGroup(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create node & database.
	if _, err := s.CreateNode("host0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err = s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", ReplicaN: 1, Duration: 1 * time.Hour}); err != nil {
		t.Fatal(err)
	}

	// Create policy on database.
	if sgi, err := s.CreateShardGroup("db0", "rp0", time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	} else if sgi.ID != 1 {
		t.Fatalf("unexpected shard group: %#v", sgi)
	}
}

// Ensure the store can delete an existing shard group.
func TestStore_DeleteShardGroup(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create node, database, policy, & group.
	if _, err := s.CreateNode("host0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err = s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", ReplicaN: 1, Duration: 1 * time.Hour}); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateShardGroup("db0", "rp0", time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	// Remove policy from database.
	if err := s.DeleteShardGroup("db0", "rp0", 1); err != nil {
		t.Fatal(err)
	}
}

// Ensure the store can create a new continuous query.
func TestStore_CreateContinuousQuery(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create query.
	if err := s.CreateContinuousQuery("SELECT count() FROM foo"); err != nil {
		t.Fatal(err)
	}
}

// Ensure that creating an existing continuous query returns an error.
func TestStore_CreateContinuousQuery_ErrContinuousQueryExists(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create continuous query.
	if err := s.CreateContinuousQuery("SELECT count() FROM foo"); err != nil {
		t.Fatal(err)
	}

	// Create it again.
	if err := s.CreateContinuousQuery("SELECT count() FROM foo"); err != meta.ErrContinuousQueryExists {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the store can delete a continuous query.
func TestStore_DropContinuousQuery(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create queries.
	if err := s.CreateContinuousQuery("SELECT count() FROM foo"); err != nil {
		t.Fatal(err)
	} else if err = s.CreateContinuousQuery("SELECT count() FROM bar"); err != nil {
		t.Fatal(err)
	} else if err = s.CreateContinuousQuery("SELECT count() FROM baz"); err != nil {
		t.Fatal(err)
	}

	// Remove one of the queries.
	if err := s.DropContinuousQuery("SELECT count() FROM bar"); err != nil {
		t.Fatal(err)
	}

	// Ensure the resulting queries are correct.
	if a, err := s.ContinuousQueries(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a, []meta.ContinuousQueryInfo{
		{Query: "SELECT count() FROM foo"},
		{Query: "SELECT count() FROM baz"},
	}) {
		t.Fatalf("unexpected queries: %#v", a)
	}
}

// Ensure the store can create a user.
func TestStore_CreateUser(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create user.
	if ui, err := s.CreateUser("susy", "pass", true); err != nil {
		t.Fatal(err)
	} else if ui.Name != "susy" || ui.Hash == "" || ui.Admin != true {
		t.Fatalf("unexpected user: %#v", ui)
	}
}

// Ensure the store can remove a user.
func TestStore_DropUser(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create users.
	if _, err := s.CreateUser("susy", "pass", true); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateUser("bob", "pass", true); err != nil {
		t.Fatal(err)
	}

	// Remove user.
	if err := s.DropUser("bob"); err != nil {
		t.Fatal(err)
	}

	// Verify user was removed.
	if a, err := s.Users(); err != nil {
		t.Fatal(err)
	} else if len(a) != 1 {
		t.Fatalf("unexpected user count: %d", len(a))
	} else if a[0].Name != "susy" {
		t.Fatalf("unexpected user: %s", a[0].Name)
	}
}

// Ensure the store can update a user.
func TestStore_UpdateUser(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	// Create users.
	if _, err := s.CreateUser("susy", "pass", true); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateUser("bob", "pass", true); err != nil {
		t.Fatal(err)
	}

	// Store password hash for bob.
	ui, err := s.User("bob")
	if err != nil {
		t.Fatal(err)
	}

	// Update user.
	if err := s.UpdateUser("bob", "XXX"); err != nil {
		t.Fatal(err)
	}

	// Verify password hash was updated.
	if other, err := s.User("bob"); err != nil {
		t.Fatal(err)
	} else if ui.Hash == other.Hash {
		t.Fatal("password hash did not change")
	}
}

// Store is a test wrapper for meta.Store.
type Store struct {
	*meta.Store
	Stderr bytes.Buffer
}

// NewStore returns a new test wrapper for Store.
func NewStore() *Store {
	s := &Store{
		Store: meta.NewStore(),
	}
	s.HeartbeatTimeout = 50 * time.Millisecond
	s.ElectionTimeout = 50 * time.Millisecond
	s.LeaderLeaseTimeout = 50 * time.Millisecond
	s.CommitTimeout = 5 * time.Millisecond
	s.Logger = log.New(&s.Stderr, "", log.LstdFlags)
	return s
}

// MustOpenStore opens a store in a temporary path. Panic on error.
func MustOpenStore() *Store {
	s := NewStore()
	if err := s.Open(MustTempFile()); err != nil {
		panic(err.Error())
	}
	return s
}

func (s *Store) Close() error {
	defer os.RemoveAll(s.Path())
	return s.Store.Close()
}

// MustTempFile returns the path to a non-existent temporary file.
func MustTempFile() string {
	f, _ := ioutil.TempFile("", "influxdb-meta-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
