package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	// DefaultRootPassword is the password initially set for the root user.
	// It is also used when reseting the root user's password.
	DefaultRootPassword = "root"

	// When planning a select statement, passing zero tells it not to chunk results. Only applies to raw queries
	NoChunkingSize = 0
)

// Service represents a long running task that is manged by a Server
type Service interface {
	Open() error
	Close() error
}

// QueryExecutor executes a query across multiple data nodes
type QueryExecutor interface {
	Execute(q *QueryRequest) (chan *influxql.Result, error)
}

// QueryRequest represent a request to run a query across the cluster
type QueryRequest struct {
	Query     *influxql.Query
	Database  string
	User      *meta.UserInfo
	ChunkSize int
}

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu       sync.RWMutex
	id       uint64
	path     string
	opened   bool
	done     chan struct{} // goroutine close notification
	rpDone   chan struct{} // retention policies goroutine close notification
	sgpcDone chan struct{} // shard group pre-create goroutine close notification

	index  uint64           // highest broadcast index seen
	errors map[uint64]error // message errors

	stats      *Stats
	Logger     *log.Logger
	WriteTrace bool // Detailed logging of write path

	authenticationEnabled bool

	// Retention policy settings
	RetentionAutoCreate bool

	// Continuous query settings
	RecomputePreviousN     int
	RecomputeNoOlderThan   time.Duration
	ComputeRunsPerInterval int
	ComputeNoMoreThan      time.Duration

	// This is the last time this data node has run continuous queries.
	// Keep this state in memory so if a broker makes a request in another second
	// to compute, it won't rerun CQs that have already been run. If this data node
	// is just getting the request after being off duty for running CQs then
	// it will recompute all of them
	lastContinuousQueryRun time.Time

	// Build information.
	Version    string
	CommitHash string

	// The meta store for accessing and updating cluster and schema data.
	MetaStore interface {
		Open() error
		Close() error
		LeaderCh() <-chan bool

		Node(id uint64) (*meta.NodeInfo, error)
		NodeByHost(host string) (*meta.NodeInfo, error)
		Nodes() ([]meta.NodeInfo, error)
		CreateNode(host string) (*meta.NodeInfo, error) // *
		DeleteNode(id uint64) error

		Database(name string) (*meta.DatabaseInfo, error)
		Databases() ([]meta.DatabaseInfo, error)
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
		CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error)
		DropDatabase(name string) error

		RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error)
		DefaultRetentionPolicy(database string) (*meta.RetentionPolicyInfo, error)
		RetentionPolicies(database string) ([]meta.RetentionPolicyInfo, error)
		CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
		CreateRetentionPolicyIfNotExists(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error)
		UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error
		SetDefaultRetentionPolicy(database, name string) error
		DropRetentionPolicy(database, name string) error

		CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
		DeleteShardGroup(database, policy string, id uint64) error

		User(name string) (*meta.UserInfo, error)
		Users() ([]meta.UserInfo, error)
		AdminUserExists() (bool, error)
		Authenticate(username, password string) (*meta.UserInfo, error)
		CreateUser(name, password string, admin bool) (*meta.UserInfo, error)
		UpdateUser(name, password string) error
		DropUser(name string) error
		SetPrivilege(username, database string, p influxql.Privilege) error

		CreateContinuousQuery(database, name, query string) error
		DropContinuousQuery(database, name string) error
	}

	// Executes statements relating to meta data.
	MetaStatementExecutor interface {
		ExecuteStatement(stmt influxql.Statement, user *meta.UserInfo) *influxql.Result
	}

	// The local data store that manages local shard data.
	DataStore interface {
		Open() error
		Close() error
	}

	// The services running on this node
	services []Service

	// Handles write request for local and remote nodes
	pw cluster.PointsWriter

	// Handles queries for local and remote nodes
	//qe QueryExecutor
}

// NewServer returns a new instance of Server.
func NewServer(path string) *Server {
	if path == "" {
		panic("path required")
	}

	metaStore := meta.NewStore(filepath.Join(path, "meta"))
	tsdbStore := tsdb.NewStore(filepath.Join(path, "data"))

	s := Server{
		path:      path,
		MetaStore: metaStore,
		DataStore: tsdbStore,

		stats:  NewStats("server"),
		Logger: log.New(os.Stderr, "[server] ", log.LstdFlags),

		// Server will always return with authentication enabled.
		// This ensures that disabling authentication must be an explicit decision.
		// To set the server to 'authless mode', call server.SetAuthenticationEnabled(false).
		authenticationEnabled: true,
	}

	s.MetaStatementExecutor = &meta.StatementExecutor{Store: metaStore}

	return &s
}

// SetAuthenticationEnabled turns on or off server authentication
func (s *Server) SetAuthenticationEnabled(enabled bool) {
	s.authenticationEnabled = enabled
}

// ID returns the data node id for the server.
// Returns zero if the server is closed or the server has not joined a cluster.
func (s *Server) ID() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// Path returns the path used when opening the server.
// Returns an empty string when the server is closed.
func (s *Server) Path() string { return s.path }

// metaPath returns the path for the metastore.
func (s *Server) metaPath() string { return filepath.Join(s.path, "meta") }

// idPath returns the path to the server's node id.
func (s *Server) idPath() string { return filepath.Join(s.path, "id") }

// Open initializes the server from a given path.
func (s *Server) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := func() error {
		// Ensure the server isn't already open and there's a path provided.
		if s.opened {
			return ErrServerOpen
		}
		s.opened = true

		// Create required directories.
		if err := os.MkdirAll(s.path, 0777); err != nil {
			return err
		}

		// Read server's node id.
		if b, err := ioutil.ReadFile(s.idPath()); os.IsNotExist(err) {
			s.id = 0
		} else if err != nil {
			return fmt.Errorf("read id: %s", err)
		} else if id, err := strconv.ParseUint(string(b), 10, 64); err != nil {
			return fmt.Errorf("parse id: %s", err)
		} else {
			s.id = id
		}

		// Open metadata store.
		if err := s.MetaStore.Open(); err != nil {
			return fmt.Errorf("meta: %s", err)
		}
		<-s.MetaStore.LeaderCh()

		// Open the local data node.
		if err := s.DataStore.Open(); err != nil {
			return fmt.Errorf("open data store: %s", err)
		}

		// Open remainging services listeners
		for _, n := range s.services {
			if err := n.Open(); err != nil {
				return fmt.Errorf("service: %s", err)
			}
		}

		return nil

	}(); err != nil {
		_ = s.close()
		return err
	}

	return nil
}

// Close shuts down the server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close()
}

func (s *Server) close() error {
	if !s.opened {
		return ErrServerClosed
	}
	s.opened = false

	for _, n := range s.services {
		if err := n.Close(); err != nil {
			return err
		}
	}

	if s.rpDone != nil {
		close(s.rpDone)
		s.rpDone = nil
	}

	if s.sgpcDone != nil {
		close(s.sgpcDone)
		s.sgpcDone = nil
	}

	_ = s.DataStore.Close()
	_ = s.MetaStore.Close()

	return nil
}

func (s *Server) URL() (url.URL, error) {
	ui, err := s.MetaStore.Node(s.id)
	if err != nil {
		return url.URL{}, err
	} else if ui == nil {
		return url.URL{}, meta.ErrNodeNotFound
	}
	return url.URL{Scheme: "http", Host: ui.Host}, nil
}

// StartSelfMonitoring starts a goroutine which monitors the InfluxDB server
// itself and stores the results in the specified database at a given interval.
func (s *Server) StartSelfMonitoring(database, retention string, interval time.Duration) error {
	/*
		if interval == 0 {
			return fmt.Errorf("statistics check interval must be non-zero")
		}

		go func() {
			tick := time.NewTicker(interval)
			for {
				<-tick.C

				// Create the batch and tags
				tags := map[string]string{"serverID": strconv.FormatUint(s.ID(), 10)}
				if h, err := os.Hostname(); err == nil {
					tags["host"] = h
				}
				batch := pointsFromStats(s.stats, tags)

				// Shard-level stats.
				tags["shardID"] = strconv.FormatUint(s.id, 10)
				s.mu.RLock()
				for _, sh := range s.shards {
					if !sh.HasDataNodeID(s.id) {
						// No stats for non-local shards.
						continue
					}
					batch = append(batch, pointsFromStats(sh.stats, tags)...)
				}
				s.mu.RUnlock()

				// Server diagnostics.
				for _, row := range s.DiagnosticsAsRows() {
					points, err := s.convertRowToPoints(row.Name, row)
					if err != nil {
						s.Logger.Printf("failed to write diagnostic row for %s: %s", row.Name, err.Error())
						continue
					}
					for _, p := range points {
						p.AddTag("serverID", strconv.FormatUint(s.ID(), 10))
					}
					batch = append(batch, points...)
				}

				s.WriteSeries(database, retention, batch)
			}
		}()
	*/
	return nil
}

// Function for local use turns stats into a slice of points
func pointsFromStats(st *Stats, tags map[string]string) []tsdb.Point {
	var points []tsdb.Point
	now := time.Now()
	st.Walk(func(k string, v int64) {
		point := tsdb.NewPoint(
			st.name+"_"+k,
			make(map[string]string),
			map[string]interface{}{"value": int(v)},
			now,
		)
		// Specifically create a new map.
		for k, v := range tags {
			tags[k] = v
			point.AddTag(k, v)
		}
		points = append(points, point)
	})

	return points
}

// StartRetentionPolicyEnforcement launches retention policy enforcement.
func (s *Server) StartRetentionPolicyEnforcement(checkInterval time.Duration) error {
	if checkInterval == 0 {
		return fmt.Errorf("retention policy check interval must be non-zero")
	}
	rpDone := make(chan struct{}, 0)
	s.rpDone = rpDone
	go func() {
		for {
			select {
			case <-rpDone:
				return
			case <-time.After(checkInterval):
				s.EnforceRetentionPolicies()
			}
		}
	}()
	return nil
}

// EnforceRetentionPolicies ensures that data that is aging-out due to retention policies
// is removed from the server.
func (s *Server) EnforceRetentionPolicies() {
	panic("not yet implemented")
	/*
		log.Println("retention policy enforcement check commencing")

		type group struct {
			Database  string
			Retention string
			ID        uint64
		}

		var groups []group
		// Only keep the lock while walking the shard groups, so the lock is not held while
		// any deletions take place across the cluster.
		func() {
			s.mu.RLock()
			defer s.mu.RUnlock()

			// Check all shard groups.
			for _, db := range s.databases {
				for _, rp := range db.policies {
					for _, g := range rp.shardGroups {
						if rp.Duration != 0 && g.EndTime.Add(rp.Duration).Before(time.Now().UTC()) {
							log.Printf("shard group %d, retention policy %s, database %s due for deletion",
								g.ID, rp.Name, db.name)
							groups = append(groups, group{Database: db.name, Retention: rp.Name, ID: g.ID})
						}
					}
				}
			}
		}()

		for _, g := range groups {
			if err := s.DeleteShardGroup(g.Database, g.Retention, g.ID); err != nil {
				log.Printf("failed to request deletion of shard group %d: %s", g.ID, err.Error())
			}
		}
	*/
}

// StartShardGroupsPreCreate launches shard group pre-create to avoid write bottlenecks.
func (s *Server) StartShardGroupsPreCreate(checkInterval time.Duration) error {
	if checkInterval == 0 {
		return fmt.Errorf("shard group pre-create check interval must be non-zero")
	}
	sgpcDone := make(chan struct{}, 0)
	s.sgpcDone = sgpcDone
	go func() {
		for {
			select {
			case <-sgpcDone:
				return
			case <-time.After(checkInterval):
				s.ShardGroupPreCreate(checkInterval)
			}
		}
	}()
	return nil
}

// ShardGroupPreCreate ensures that future shard groups and shards are created and ready for writing
// is removed from the server.
func (s *Server) ShardGroupPreCreate(checkInterval time.Duration) {
	panic("not yet implemented")

	/*
		log.Println("shard group pre-create check commencing")

		// For safety, we double the check interval to ensure we have enough time to create all shard groups
		// before they are needed, but as close to needed as possible.
		// This is a complete punt on optimization
		cutoff := time.Now().Add(checkInterval * 2).UTC()

		type group struct {
			Database  string
			Retention string
			ID        uint64
			Time      time.Time
		}

		var groups []group
		// Only keep the lock while walking the shard groups, so the lock is not held while
		// any deletions take place across the cluster.
		func() {
			s.mu.RLock()
			defer s.mu.RUnlock()

			// Check all shard groups.
			// See if they have a "future" shard group ready to write to
			// If not, create the next shard group, as well as each shard for the shardGroup
			for _, db := range s.databases {
				for _, rp := range db.policies {
					for _, g := range rp.shardGroups {
						// Check to see if it is going to end before our interval
						if g.EndTime.Before(cutoff) {
							log.Printf("pre-creating shard group for %d, retention policy %s, database %s", g.ID, rp.Name, db.name)
							groups = append(groups, group{Database: db.name, Retention: rp.Name, ID: g.ID, Time: g.EndTime.Add(1 * time.Nanosecond)})
						}
					}
				}
			}
		}()

		for _, g := range groups {
			if err := s.CreateShardGroupIfNotExists(g.Database, g.Retention, g.Time); err != nil {
				log.Printf("failed to request pre-creation of shard group %d for time %s: %s", g.ID, g.Time, err.Error())
			}
		}
	*/
}

// Initialize creates a new data node and initializes the server's id to the latest.
func (s *Server) Initialize(u url.URL) error {
	// Create node in meta store.
	ni, err := s.MetaStore.CreateNode(u.Host)
	if err != nil {
		return fmt.Errorf("create node: %s", err)
	}

	// Write id to file.
	if err := ioutil.WriteFile(s.idPath(), []byte(strconv.FormatUint(ni.ID, 10)), 0666); err != nil {
		return fmt.Errorf("write id: %s", err)
	}

	return nil
}

// Join creates a new data node in an existing cluster, copies the metastore,
// and initializes the ID.
func (s *Server) Join(u url.URL, joinURL url.URL) error {
	panic("FIXME: join to cluster")

	/*
		s.mu.Lock()
		defer s.mu.Unlock()

		// Create the initial request.
		// Might get a redirect though depending on the nodes role
		joinURL.Path = "/data/data_nodes"

		var retries int
		var resp *http.Response
		var err error

		// When POSTing the to the join endpoint, we are manually following redirects
		// and not relying on the Go http client redirect policy. The Go http client will convert
		// POSTs to GETSs when following redirects which is not what we want when joining.
		// (i.e. we want to join a node, not list the nodes) If we receive a redirect response,
		// the Location header is where we should resend the POST.  We also need to re-encode
		// body since the buf was already read.
		for {
			// Should never get here but bail to avoid a infinite redirect loop to be safe
			if retries >= 60 {
				return ErrUnableToJoin
			}

			// Encode data node request.
			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(&dataNodeJSON{URL: u.String()}); err != nil {
				return err
			}

			resp, err = http.Post(joinURL.String(), "application/octet-stream", &buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			// If we get a service unavailable, the other data nodes may still be booting
			// so retry again
			if resp.StatusCode == http.StatusServiceUnavailable {
				s.Logger.Printf("join unavailable, retrying")
				retries += 1
				time.Sleep(1 * time.Second)
				continue
			}

			// We likely tried to join onto a broker which cannot handle this request.  It
			// has given us the address of a known data node to join instead.
			if resp.StatusCode == http.StatusTemporaryRedirect {
				redirectURL, err := url.Parse(resp.Header.Get("Location"))
				s.Logger.Printf("redirect join: %s", redirectURL)

				// if we happen to get redirected back to ourselves then we'll never join.  This
				// may because the heartbeater could have already fired once, registering our endpoints
				// as a data node and the broker is redirecting data node requests back to us.  In
				// this case, just re-request the original URL again util we get a different node.
				if redirectURL.Host != u.Host {
					joinURL = redirectURL
				}
				if err != nil {
					return err
				}
				retries += 1
				resp.Body.Close()
				continue
			}

			// If we are first data node, we can't join anyone and need to initialize
			if resp.StatusCode == http.StatusNotFound {
				return ErrDataNodeNotFound
			}
			break
		}

		// Check if created.
		if resp.StatusCode != http.StatusCreated {
			return ErrUnableToJoin
		}

		// Decode response.
		var n dataNodeJSON
		if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
			return err
		}
		assert(n.ID > 0, "invalid join node id returned: %d", n.ID)

		// Download the metastore from joining server.
		joinURL.Path = "/data/metastore"
		resp, err = http.Get(joinURL.String())
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Check response & parse content length.
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unsuccessful meta copy: status=%d (%s)", resp.StatusCode, joinURL.String())
		}
		sz, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse meta size: %s", err)
		}

		// Close the metastore.
		_ = s.meta.close()

		// Overwrite the metastore.
		f, err := os.Create(s.metaPath())
		if err != nil {
			return fmt.Errorf("create meta file: %s", err)
		}

		// Copy and check size.
		if _, err := io.CopyN(f, resp.Body, sz); err != nil {
			_ = f.Close()
			return fmt.Errorf("copy meta file: %s", err)
		}
		_ = f.Close()

		// Reopen metastore.
		s.meta = &metastore{}
		if err := s.meta.open(s.metaPath()); err != nil {
			return fmt.Errorf("reopen meta: %s", err)
		}

		// Update the ID on the metastore.
		if err := s.meta.mustUpdate(0, func(tx *metatx) error {
			return tx.setID(n.ID)
		}); err != nil {
			return err
		}

		// Reload the server.
		log.Printf("reloading metadata")
		if err := s.load(); err != nil {
			return fmt.Errorf("reload: %s", err)
		}

		return nil
	*/
}

// Node returns a node by id.
func (s *Server) Node(id uint64) (*meta.NodeInfo, error) {
	return s.MetaStore.Node(id)
}

// NodesByID returns the nodes matching the passed ids.
func (s *Server) NodesByID(ids []uint64) ([]meta.NodeInfo, error) {
	nodes, err := s.MetaStore.Nodes()
	if err != nil {
		return nil, err
	}

	var a []meta.NodeInfo
	for i := range nodes {
		a = append(a, nodes[i])
	}
	return a, nil
}

// NodeByURL returns a node by url.
func (s *Server) NodeByURL(u *url.URL) (*meta.NodeInfo, error) {
	return s.MetaStore.NodeByHost(u.Host)
}

// Nodes returns a list of all nodes in the cluster.
func (s *Server) Nodes() ([]meta.NodeInfo, error) {
	return s.MetaStore.Nodes()
}

// CreateNode creates a new node in the cluster.
func (s *Server) CreateNode(u *url.URL) error {
	_, err := s.MetaStore.CreateNode(u.Host)
	return err
}

// DeleteNode deletes an existing node in the cluster.
func (s *Server) DeleteNode(id uint64) error {
	return s.MetaStore.DeleteNode(id)
}

// DatabaseExists returns true if a database exists.
func (s *Server) DatabaseExists(name string) (bool, error) {
	di, err := s.MetaStore.Database(name)
	if err != nil {
		return false, err
	}
	return di != nil, nil
}

// Databases returns a sorted list of all database names.
func (s *Server) Databases() ([]string, error) {
	dis, err := s.MetaStore.Databases()
	if err != nil {
		return nil, err
	}

	a := make([]string, len(dis))
	for i := range dis {
		a[i] = dis[i].Name
	}
	sort.Strings(a)

	return a, nil
}

// CreateDatabase creates a new database.
func (s *Server) CreateDatabase(name string) error {
	_, err := s.MetaStore.CreateDatabase(name)
	return err
}

// CreateDatabaseIfNotExists creates a new database if, and only if, it does not exist already.
func (s *Server) CreateDatabaseIfNotExists(name string) error {
	_, err := s.MetaStore.CreateDatabaseIfNotExists(name)
	return err
}

// DropDatabase deletes an existing database.
func (s *Server) DropDatabase(name string) error {
	return s.MetaStore.DropDatabase(name)
}

// CreateShardGroupIfNotExists creates the shard group for a retention policy for the interval a timestamp falls into.
func (s *Server) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) error {
	_, err := s.MetaStore.CreateShardGroupIfNotExists(database, policy, timestamp)
	return err
}

// DeleteShardGroup deletes the shard group identified by id.
func (s *Server) DeleteShardGroup(database, policy string, id uint64) error {
	return s.MetaStore.DeleteShardGroup(database, policy, id)
}

// User returns a user by username
// Returns nil if the user does not exist.
func (s *Server) User(name string) (*meta.UserInfo, error) {
	return s.MetaStore.User(name)
}

// Users returns a list of all users, sorted by name.
func (s *Server) Users() ([]meta.UserInfo, error) {
	return s.MetaStore.Users()
}

// AdminUserExists returns whether at least 1 admin-level user exists.
func (s *Server) AdminUserExists() (bool, error) {
	return s.MetaStore.AdminUserExists()
}

// Authenticate returns an authenticated user by username.
// Returns an error if authentication fails.
func (s *Server) Authenticate(username, password string) (*meta.UserInfo, error) {
	return s.MetaStore.Authenticate(username, password)
}

// CreateUser creates a user on the server.
func (s *Server) CreateUser(username, password string, admin bool) error {
	_, err := s.MetaStore.CreateUser(username, password, admin)
	return err
}

// UpdateUser updates an existing user on the server.
func (s *Server) UpdateUser(username, password string) error {
	return s.MetaStore.UpdateUser(username, password)
}

// DropUser removes a user from the server.
func (s *Server) DropUser(username string) error {
	return s.MetaStore.DropUser(username)
}

// SetPrivilege grants / revokes a privilege to a user.
func (s *Server) SetPrivilege(username, database string, p influxql.Privilege) error {
	return s.MetaStore.SetPrivilege(username, database, p)
}

// RetentionPolicy returns a retention policy by name.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return s.MetaStore.RetentionPolicy(database, name)
}

// DefaultRetentionPolicy returns the default retention policy for a database.
// Returns an error if the database doesn't exist.
func (s *Server) DefaultRetentionPolicy(database string) (*meta.RetentionPolicyInfo, error) {
	return s.MetaStore.DefaultRetentionPolicy(database)
}

// RetentionPolicies returns a list of retention polocies for a database.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicies(database string) ([]meta.RetentionPolicyInfo, error) {
	return s.MetaStore.RetentionPolicies(database)
}

// RetentionPolicyExists returns true if a retention policy exists for a given database.
func (s *Server) RetentionPolicyExists(database, policy string) (bool, error) {
	rpi, err := s.MetaStore.RetentionPolicy(database, policy)
	if err != nil {
		return false, err
	}
	return rpi == nil, nil
}

// CreateRetentionPolicy creates a retention policy for a database.
func (s *Server) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) error {
	_, err := s.MetaStore.CreateRetentionPolicy(database, rpi)
	return err
}

// CreateRetentionPolicyIfNotExists creates a retention policy for a database.
func (s *Server) CreateRetentionPolicyIfNotExists(database string, rpi *meta.RetentionPolicyInfo) error {
	_, err := s.MetaStore.CreateRetentionPolicyIfNotExists(database, rpi)
	return err
}

// UpdateRetentionPolicy updates an existing retention policy on a database.
func (s *Server) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error {
	return s.MetaStore.UpdateRetentionPolicy(database, name, rpu)
}

// DropRetentionPolicy removes a retention policy from a database.
func (s *Server) DropRetentionPolicy(database, name string) error {
	return s.MetaStore.DropRetentionPolicy(database, name)
}

// SetDefaultRetentionPolicy sets the default policy to write data into and query from on a database.
func (s *Server) SetDefaultRetentionPolicy(database, name string) error {
	return s.MetaStore.SetDefaultRetentionPolicy(database, name)
}

// DropSeries deletes from an existing series.
func (s *Server) DropSeries(database string, seriesByMeasurement map[string][]uint64) error {
	panic("not yet implemented")
}

// WriteSeries writes series data to the database.
// Returns the messaging index the data was written to.
func (s *Server) WriteSeries(database, retentionPolicy string, points []tsdb.Point) (idx uint64, err error) {
	return 0, s.pw.Write(&cluster.WritePointsRequest{
		Database:         database,
		RetentionPolicy:  retentionPolicy,
		ConsistencyLevel: cluster.ConsistencyLevelAll,
		Points:           points,
	})
}

// DropMeasurement drops a given measurement from a database.
func (s *Server) DropMeasurement(database, name string) error {
	panic("not yet implemented")
}

// ExecuteQuery executes an InfluxQL query against the server.
// If the user isn't authorized to access the database an error will be returned.
// It sends results down the passed in chan and closes it when done. It will close the chan
// on the first statement that throws an error.
func (s *Server) ExecuteQuery(q *influxql.Query, database string, user *meta.UserInfo, chunkSize int) (chan *influxql.Result, error) {
	// Authorize user to execute the query.
	if s.authenticationEnabled {
		if err := s.Authorize(user, q, database); err != nil {
			return nil, err
		}
	}

	s.stats.Add("queriesRx", int64(len(q.Statements)))

	// Execute each statement. Keep the iterator external so we can
	// track how many of the statements were executed
	results := make(chan *influxql.Result)
	go func() {
		var i int
		var stmt influxql.Statement
		for i, stmt = range q.Statements {
			// If a default database wasn't passed in by the caller,
			// try to get it from the statement.
			defaultDB := database
			if defaultDB == "" {
				if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
					defaultDB = s.DefaultDatabase()
				}

			}

			// If we have a default database, normalize the statement with it.
			if defaultDB != "" {
				if err := s.NormalizeStatement(stmt, defaultDB); err != nil {
					results <- &influxql.Result{Err: err}
					break
				}
			}

			var res *influxql.Result
			switch stmt := stmt.(type) {
			case *influxql.SelectStatement:
				panic("not yet implemented")
				// if err := s.executeSelectStatement(i, stmt, database, user, results, chunkSize); err != nil {
				// 	results <- &influxql.Result{Err: err}
				// 	break
				// }
			case *influxql.DropSeriesStatement:
				res = s.executeDropSeriesStatement(stmt, database, user)
			case *influxql.ShowSeriesStatement:
				res = s.executeShowSeriesStatement(stmt, database, user)
			case *influxql.DropMeasurementStatement:
				res = s.executeDropMeasurementStatement(stmt, database, user)
			case *influxql.ShowMeasurementsStatement:
				res = s.executeShowMeasurementsStatement(stmt, database, user)
			case *influxql.ShowTagKeysStatement:
				res = s.executeShowTagKeysStatement(stmt, database, user)
			case *influxql.ShowTagValuesStatement:
				res = s.executeShowTagValuesStatement(stmt, database, user)
			case *influxql.ShowFieldKeysStatement:
				res = s.executeShowFieldKeysStatement(stmt, database, user)
			case *influxql.ShowStatsStatement:
				res = s.executeShowStatsStatement(stmt, user)
			case *influxql.ShowDiagnosticsStatement:
				res = s.executeShowDiagnosticsStatement(stmt, user)
			case *influxql.DeleteStatement:
				res = &influxql.Result{Err: ErrInvalidQuery}
			default:
				// Delegate all meta statements to a separate executor.
				res = s.MetaStatementExecutor.ExecuteStatement(stmt, user)
			}

			if res != nil {
				// set the StatementID for the handler on the other side to combine results
				res.StatementID = i

				// If an error occurs then stop processing remaining statements.
				results <- res
				if res.Err != nil {
					break
				}
			}

			s.stats.Inc("queriesExecuted")
		}

		// if there was an error send results that the remaining statements weren't executed
		for ; i < len(q.Statements)-1; i++ {
			results <- &influxql.Result{Err: ErrNotExecuted}
		}

		close(results)
	}()

	return results, nil
}

// executeSelectStatement plans and executes a select statement against a database.
/*
func (s *Server) executeSelectStatement(statementID int, stmt *influxql.SelectStatement, database string, user *meta.UserInfo, results chan *influxql.Result, chunkSize int) error {
	// Perform any necessary query re-writing.
	stmt, err := s.rewriteSelectStatement(stmt)
	if err != nil {
		return err
	}

	// Plan statement execution.
	e, err := s.planSelectStatement(stmt, chunkSize)
	if err != nil {
		return err
	}

	// Execute plan.
	ch := e.Execute()

	// Stream results from the channel. We should send an empty result if nothing comes through.
	resultSent := false
	for row := range ch {
		if row.Err != nil {
			return row.Err
		} else {
			resultSent = true
			results <- &influxql.Result{StatementID: statementID, Series: []*influxql.Row{row}}
		}
	}

	if !resultSent {
		results <- &influxql.Result{StatementID: statementID, Series: make([]*influxql.Row, 0)}
	}

	return nil
}
*/

// rewriteSelectStatement performs any necessary query re-writing.
func (s *Server) rewriteSelectStatement(stmt *influxql.SelectStatement) (*influxql.SelectStatement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var err error

	// Expand regex expressions in the FROM clause.
	sources, err := s.expandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}
	stmt.Sources = sources

	// Expand wildcards in the fields or GROUP BY.
	if stmt.HasWildcard() {
		stmt, err = s.expandWildcards(stmt)
		if err != nil {
			return nil, err
		}
	}

	stmt.RewriteDistinct()

	return stmt, nil
}

// expandWildcards returns a new SelectStatement with wildcards in the fields
// and/or GROUP BY exapnded with actual field names.
func (s *Server) expandWildcards(stmt *influxql.SelectStatement) (*influxql.SelectStatement, error) {
	// If there are no wildcards in the statement, return it as-is.
	if !stmt.HasWildcard() {
		return stmt, nil
	}

	panic("not yet implemented")
	/*
		// Use sets to avoid duplicate field names.
		fieldSet := map[string]struct{}{}
		dimensionSet := map[string]struct{}{}

		var fields influxql.Fields
		var dimensions influxql.Dimensions

		// Iterate measurements in the FROM clause getting the fields & dimensions for each.
		for _, src := range stmt.Sources {
			if m, ok := src.(*influxql.Measurement); ok {
				// Lookup the database.
				db, ok := s.databases[m.Database]
				if !ok {
					return nil, ErrDatabaseNotFound(m.Database)
				}

				// Lookup the measurement in the database.
				mm := db.measurements[m.Name]
				if mm == nil {
					return nil, ErrMeasurementNotFound(m.String())
				}

				// Get the fields for this measurement.
				for _, f := range mm.Fields {
					if _, ok := fieldSet[f.Name]; ok {
						continue
					}
					fieldSet[f.Name] = struct{}{}
					fields = append(fields, &influxql.Field{Expr: &influxql.VarRef{Val: f.Name}})
				}

				// Get the dimensions for this measurement.
				for _, t := range mm.tagKeys() {
					if _, ok := dimensionSet[t]; ok {
						continue
					}
					dimensionSet[t] = struct{}{}
					dimensions = append(dimensions, &influxql.Dimension{Expr: &influxql.VarRef{Val: t}})
				}
			}
		}

		// Return a new SelectStatement with the wild cards rewritten.
		return stmt.RewriteWildcards(fields, dimensions), nil
	*/
}

// expandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Server) expandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates. Two regexes might produce
	// duplicates when expanded.
	set := map[string]influxql.Source{}
	names := []string{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *influxql.Measurement:
			if src.Regex == nil {
				name := src.String()
				set[name] = src
				names = append(names, name)
				continue
			}

			panic("not yet implemented")
			/*
				// Lookup the database.
				db := s.databases[src.Database]
				if db == nil {
					return nil, ErrDatabaseNotFound(src.Database)
				}

				// Get measurements from the database that match the regex.
				measurements := db.measurementsByRegex(src.Regex.Val)

				// Add those measurments to the set.
				for _, m := range measurements {
					m2 := &influxql.Measurement{
						Database:        src.Database,
						RetentionPolicy: src.RetentionPolicy,
						Name:            m.Name,
					}

					name := m2.String()
					if _, ok := set[name]; !ok {
						set[name] = m2
						names = append(names, name)
					}
				}
			*/

		default:
			return nil, fmt.Errorf("expandSources: unsuported source type: %T", source)
		}
	}

	// Sort the list of source names.
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(influxql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

// plans a selection statement under lock.
/*
func (s *Server) planSelectStatement(stmt *influxql.SelectStatement, chunkSize int) (*influxql.Executor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Plan query.
	p := influxql.NewPlanner(s)

	return p.Plan(stmt, chunkSize)
}
*/

func (s *Server) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string, user *meta.UserInfo) *influxql.Result {
	return &influxql.Result{Err: s.DropMeasurement(database, stmt.Name)}
}

func (s *Server) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
}

func (s *Server) executeShowSeriesStatement(stmt *influxql.ShowSeriesStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Find the database.
		db := s.databases[database]
		if db == nil {
			return &influxql.Result{Err: ErrDatabaseNotFound(database)}
		}

		// Get the list of measurements we're interested in.
		measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
		if err != nil {
			return &influxql.Result{Err: err}
		}

		// Create result struct that will be populated and returned.
		result := &influxql.Result{
			Series: make(influxql.Rows, 0, len(measurements)),
		}

		// Loop through measurements to build result. One result row / measurement.
		for _, m := range measurements {
			var ids seriesIDs

			if stmt.Condition != nil {
				// Get series IDs that match the WHERE clause.
				filters := map[uint64]influxql.Expr{}
				ids, _, _, err = m.walkWhereForSeriesIds(stmt.Condition, filters)
				if err != nil {
					return &influxql.Result{Err: err}
				}

				// If no series matched, then go to the next measurement.
				if len(ids) == 0 {
					continue
				}

				// TODO: check return of walkWhereForSeriesIds for fields
			} else {
				// No WHERE clause so get all series IDs for this measurement.
				ids = m.seriesIDs
			}

			// Make a new row for this measurement.
			r := &influxql.Row{
				Name:    m.Name,
				Columns: m.tagKeys(),
			}

			// Loop through series IDs getting matching tag sets.
			for _, id := range ids {
				if s, ok := m.seriesByID[id]; ok {
					values := make([]interface{}, 0, len(r.Columns)+1)
					values = append(values, id)
					for _, column := range r.Columns {
						values = append(values, s.Tags[column])
					}

					// Add the tag values to the row.
					r.Values = append(r.Values, values)
				}
			}
			// make the id the first column
			r.Columns = append([]string{"_id"}, r.Columns...)

			// Append the row to the result.
			result.Series = append(result.Series, r)
		}

		if stmt.Limit > 0 || stmt.Offset > 0 {
			result.Series = s.filterShowSeriesResult(stmt.Limit, stmt.Offset, result.Series)
		}

		return result
	*/
}

// filterShowSeriesResult will limit the number of series returned based on the limit and the offset.
// Unlike limit and offset on SELECT statements, the limit and offset don't apply to the number of Rows, but
// to the number of total Values returned, since each Value represents a unique series.
func (s *Server) filterShowSeriesResult(limit, offset int, rows influxql.Rows) influxql.Rows {
	var filteredSeries influxql.Rows
	seriesCount := 0
	for _, r := range rows {
		var currentSeries [][]interface{}

		// filter the values
		for _, v := range r.Values {
			if seriesCount >= offset && seriesCount-offset < limit {
				currentSeries = append(currentSeries, v)
			}
			seriesCount++
		}

		// only add the row back in if there are some values in it
		if len(currentSeries) > 0 {
			r.Values = currentSeries
			filteredSeries = append(filteredSeries, r)
			if seriesCount > limit+offset {
				return filteredSeries
			}
		}
	}
	return filteredSeries
}

func (s *Server) executeShowMeasurementsStatement(stmt *influxql.ShowMeasurementsStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")

	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Find the database.
		db := s.databases[database]
		if db == nil {
			return &influxql.Result{Err: ErrDatabaseNotFound(database)}
		}

		var measurements Measurements

		// If a WHERE clause was specified, filter the measurements.
		if stmt.Condition != nil {
			var err error
			measurements, err = db.measurementsByExpr(stmt.Condition)
			if err != nil {
				return &influxql.Result{Err: err}
			}
		} else {
			// Otherwise, get all measurements from the database.
			measurements = db.Measurements()
		}
		sort.Sort(measurements)

		offset := stmt.Offset
		limit := stmt.Limit

		// If OFFSET is past the end of the array, return empty results.
		if offset > len(measurements)-1 {
			return &influxql.Result{}
		}

		// Calculate last index based on LIMIT.
		end := len(measurements)
		if limit > 0 && offset+limit < end {
			limit = offset + limit
		} else {
			limit = end
		}

		// Make a result row to hold all measurement names.
		row := &influxql.Row{
			Name:    "measurements",
			Columns: []string{"name"},
		}

		// Add one value to the row for each measurement name.
		for i := offset; i < limit; i++ {
			m := measurements[i]
			v := interface{}(m.Name)
			row.Values = append(row.Values, []interface{}{v})
		}

		// Make a result.
		result := &influxql.Result{
			Series: influxql.Rows{row},
		}

		return result
	*/
}

func (s *Server) executeShowTagKeysStatement(stmt *influxql.ShowTagKeysStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Find the database.
		db := s.databases[database]
		if db == nil {
			return &influxql.Result{Err: ErrDatabaseNotFound(database)}
		}

		// Get the list of measurements we're interested in.
		measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
		if err != nil {
			return &influxql.Result{Err: err}
		}

		// Make result.
		result := &influxql.Result{
			Series: make(influxql.Rows, 0, len(measurements)),
		}

		// Add one row per measurement to the result.
		for _, m := range measurements {
			// TODO: filter tag keys by stmt.Condition

			// Get the tag keys in sorted order.
			keys := m.tagKeys()

			// Convert keys to an [][]interface{}.
			values := make([][]interface{}, 0, len(m.seriesByTagKeyValue))
			for _, k := range keys {
				v := interface{}(k)
				values = append(values, []interface{}{v})
			}

			// Make a result row for the measurement.
			r := &influxql.Row{
				Name:    m.Name,
				Columns: []string{"tagKey"},
				Values:  values,
			}

			result.Series = append(result.Series, r)
		}

		// TODO: LIMIT & OFFSET

		return result
	*/
}

func (s *Server) executeShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Find the database.
		db := s.databases[database]
		if db == nil {
			return &influxql.Result{Err: ErrDatabaseNotFound(database)}
		}

		// Get the list of measurements we're interested in.
		measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
		if err != nil {
			return &influxql.Result{Err: err}
		}

		// Make result.
		result := &influxql.Result{
			Series: make(influxql.Rows, 0),
		}

		tagValues := make(map[string]stringSet)
		for _, m := range measurements {
			var ids seriesIDs

			if stmt.Condition != nil {
				// Get series IDs that match the WHERE clause.
				filters := map[uint64]influxql.Expr{}
				ids, _, _, err = m.walkWhereForSeriesIds(stmt.Condition, filters)
				if err != nil {
					return &influxql.Result{Err: err}
				}

				// If no series matched, then go to the next measurement.
				if len(ids) == 0 {
					continue
				}

				// TODO: check return of walkWhereForSeriesIds for fields
			} else {
				// No WHERE clause so get all series IDs for this measurement.
				ids = m.seriesIDs
			}

			for k, v := range m.tagValuesByKeyAndSeriesID(stmt.TagKeys, ids) {
				_, ok := tagValues[k]
				if !ok {
					tagValues[k] = v
				}
				tagValues[k] = tagValues[k].union(v)
			}
		}

		for k, v := range tagValues {
			r := &influxql.Row{
				Name:    k + "TagValues",
				Columns: []string{k},
			}

			vals := v.list()
			sort.Strings(vals)

			for _, val := range vals {
				v := interface{}(val)
				r.Values = append(r.Values, []interface{}{v})
			}

			result.Series = append(result.Series, r)
		}

		sort.Sort(result.Series)
		return result
	*/
}

func (s *Server) executeShowStatsStatement(stmt *influxql.ShowStatsStatement, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
	/*
		var rows []*influxql.Row
		// Server stats.
		serverRow := &influxql.Row{Columns: []string{}}
		serverRow.Name = s.stats.Name()
		var values []interface{}
		s.stats.Walk(func(k string, v int64) {
			serverRow.Columns = append(serverRow.Columns, k)
			values = append(values, v)
		})
		serverRow.Values = append(serverRow.Values, values)
		rows = append(rows, serverRow)

		// Shard-level stats.
		for _, sh := range s.shards {
			if sh.store == nil {
				// No stats for non-local shards
				continue
			}

			row := &influxql.Row{Columns: []string{}}
			row.Name = sh.stats.Name()
			var values []interface{}
			sh.stats.Walk(func(k string, v int64) {
				row.Columns = append(row.Columns, k)
				values = append(values, v)
			})
			row.Values = append(row.Values, values)
			rows = append(rows, row)
		}

		return &influxql.Result{Series: rows}
	*/
}

func (s *Server) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement, user *meta.UserInfo) *influxql.Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &influxql.Result{Series: s.DiagnosticsAsRows()}
}

func (s *Server) executeShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string, user *meta.UserInfo) *influxql.Result {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		var err error

		// Find the database.
		db := s.databases[database]
		if db == nil {
			return &influxql.Result{Err: ErrDatabaseNotFound(database)}
		}

		// Get the list of measurements we're interested in.
		measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
		if err != nil {
			return &influxql.Result{Err: err}
		}

		// Make result.
		result := &influxql.Result{
			Series: make(influxql.Rows, 0, len(measurements)),
		}

		// Loop through measurements, adding a result row for each.
		for _, m := range measurements {
			// Create a new row.
			r := &influxql.Row{
				Name:    m.Name,
				Columns: []string{"fieldKey"},
			}

			// Get a list of field names from the measurement then sort them.
			names := make([]string, 0, len(m.Fields))
			for _, f := range m.Fields {
				names = append(names, f.Name)
			}
			sort.Strings(names)

			// Add the field names to the result row values.
			for _, n := range names {
				v := interface{}(n)
				r.Values = append(r.Values, []interface{}{v})
			}

			// Append the row to the result.
			result.Series = append(result.Series, r)
		}

		return result
	*/
}

// measurementsFromSourceOrDB returns a list of measurements from the
// statement passed in or, if the statement is nil, a list of all
// measurement names from the database passed in.
/*
func measurementsFromSourceOrDB(stmt influxql.Source, db *database) (Measurements, error) {
	var measurements Measurements
	if stmt != nil {
		// TODO: handle multiple measurement sources
		if m, ok := stmt.(*influxql.Measurement); ok {
			measurement := db.measurements[m.Name]
			if measurement == nil {
				return nil, ErrMeasurementNotFound(m.Name)
			}

			measurements = append(measurements, measurement)
		} else {
			return nil, errors.New("identifiers in FROM clause must be measurement names")
		}
	} else {
		// No measurements specified in FROM clause so get all measurements that have series.
		for _, m := range db.Measurements() {
			if len(m.seriesIDs) > 0 {
				measurements = append(measurements, m)
			}
		}
	}
	sort.Sort(measurements)

	return measurements, nil
}
*/

// MeasurementNames returns a list of all measurements for the specified database.
func (s *Server) MeasurementNames(database string) []string {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		db := s.databases[database]
		if db == nil {
			return nil
		}

		return db.names
	*/
}

// measurement returns a measurement by database and name.
/*
func (s *Server) measurement(database, name string) (*Measurement, error) {
	panic("not yet implemented")
		db := s.databases[database]
		if db == nil {
			return nil, ErrDatabaseNotFound(database)
		}

		return db.measurements[name], nil
}
*/

// Begin returns an unopened transaction associated with the server.
// func (s *Server) Begin() (influxql.Tx, error) { return newTx(s), nil }

// StartLocalMapper will create a local mapper for the passed in remote mapper
/*
func (s *Server) StartLocalMapper(rm *RemoteMapper) (*LocalMapper, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// get everything we need to run the local mapper
	shard := s.shards[rm.ShardID]
	if shard == nil {
		return nil, ErrShardNotFound
	}

	// this should never be the case, but we have to be sure
	if shard.store == nil {
		return nil, ErrShardNotLocal
	}

	db := s.databases[rm.Database]
	if db == nil {
		return nil, ErrDatabaseNotFound(rm.Database)
	}

	m := db.measurements[rm.MeasurementName]
	if m == nil {
		return nil, ErrMeasurementNotFound(rm.MeasurementName)
	}

	// create a job, it's only used as a container for a few variables
	job := &influxql.MapReduceJob{
		MeasurementName: rm.MeasurementName,
		TMin:            rm.TMin,
		TMax:            rm.TMax,
	}

	// limits and offsets can't be evaluated at the local mapper so we need to read
	// limit + offset points to be sure that the reducer will be able to correctly put things together
	limit := uint64(rm.Limit) + uint64(rm.Offset)
	// if limit is zero, just set to the max number since we use limit == 0 later to determine if the mapper is empty
	if limit == 0 {
		limit = math.MaxUint64
	}

	// now create and start the local mapper
	lm := &LocalMapper{
		seriesIDs:    rm.SeriesIDs,
		job:          job,
		db:           shard.store,
		decoder:      NewFieldCodec(m),
		filters:      rm.FilterExprs(),
		whereFields:  rm.WhereFields,
		selectFields: rm.SelectFields,
		selectTags:   rm.SelectTags,
		interval:     rm.Interval,
		tmin:         rm.TMin,
		tmax:         rm.TMax,
		limit:        limit,
	}

	return lm, nil
}
*/

// NormalizeStatement adds a default database and policy to the measurements in statement.
func (s *Server) NormalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.normalizeStatement(stmt, defaultDatabase)
}

func (s *Server) normalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	// Track prefixes for replacing field names.
	prefixes := make(map[string]string)

	// Qualify all measurements.
	influxql.WalkFunc(stmt, func(n influxql.Node) {
		if err != nil {
			return
		}
		switch n := n.(type) {
		case *influxql.Measurement:
			e := s.normalizeMeasurement(n, defaultDatabase)
			if e != nil {
				err = e
				return
			}
			prefixes[n.Name] = n.Name
		}
	})
	if err != nil {
		return err
	}

	// Replace all variable references that used measurement prefixes.
	influxql.WalkFunc(stmt, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.VarRef:
			for k, v := range prefixes {
				if strings.HasPrefix(n.Val, k+".") {
					n.Val = v + "." + influxql.QuoteIdent(n.Val[len(k)+1:])
				}
			}
		}
	})

	return
}

// NormalizeMeasurement inserts the default database or policy into all measurement names.
func (s *Server) NormalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.normalizeMeasurement(m, defaultDatabase)
}

func (s *Server) normalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	if defaultDatabase == "" {
		return errors.New("no default database specified")
	}
	if m.Name == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// Find database.
	di, err := s.MetaStore.Database(m.Database)
	if err != nil {
		return err
	} else if di == nil {
		return ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if di.DefaultRetentionPolicy == "" {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
		m.RetentionPolicy = di.DefaultRetentionPolicy
	}

	return nil
}

// DiagnosticsAsRows returns diagnostic information about the server, as a slice of
// InfluxQL rows.
func (s *Server) DiagnosticsAsRows() []*influxql.Row {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()
		now := time.Now().UTC()

		// Common rows.
		gd := NewGoDiagnostics()
		sd := NewSystemDiagnostics()
		md := NewMemoryDiagnostics()
		bd := BuildDiagnostics{Version: s.Version, CommitHash: s.CommitHash}

		// Common tagset.
		tags := map[string]string{"serverID": strconv.FormatUint(s.id, 10)}

		// Server row.
		serverRow := &influxql.Row{
			Name: "server_diag",
			Columns: []string{"time", "startTime", "uptime", "id",
				"path", "authEnabled", "index", "retentionAutoCreate", "numShards", "cqLastRun"},
			Tags: tags,
			Values: [][]interface{}{[]interface{}{now, startTime.String(), time.Since(startTime).String(), strconv.FormatUint(s.id, 10),
				s.path, s.authenticationEnabled, int64(s.index), s.RetentionAutoCreate, len(s.shards), s.lastContinuousQueryRun.String()}},
		}

		// Shard groups.
		shardGroupsRow := &influxql.Row{Columns: []string{}}
		shardGroupsRow.Name = "shardGroups_diag"
		shardGroupsRow.Columns = append(shardGroupsRow.Columns, "time", "database", "retentionPolicy", "id",
			"startTime", "endTime", "duration", "numShards")
		shardGroupsRow.Tags = tags
		// Check all shard groups.
		for _, db := range s.databases {
			for _, rp := range db.policies {
				for _, g := range rp.shardGroups {
					shardGroupsRow.Values = append(shardGroupsRow.Values, []interface{}{now, db.name, rp.Name,
						strconv.FormatUint(g.ID, 10), g.StartTime.String(), g.EndTime.String(), g.Duration().String(), len(g.Shards)})
				}
			}
		}

		// Shards
		shardsRow := &influxql.Row{Columns: []string{}}
		shardsRow.Name = "shards_diag"
		shardsRow.Columns = append(shardsRow.Columns, "time", "id", "dataNodes", "index", "path")
		shardsRow.Tags = tags
		for _, sh := range s.shards {
			var nodes []string
			for _, n := range sh.DataNodeIDs {
				nodes = append(nodes, strconv.FormatUint(n, 10))
			}
			var path string
			if sh.HasDataNodeID(s.id) {
				path = sh.store.Path()
			}
			shardsRow.Values = append(shardsRow.Values, []interface{}{now, strconv.FormatUint(sh.ID, 10),
				strings.Join(nodes, ","), strconv.FormatUint(sh.Index(), 10), path})
		}

		return []*influxql.Row{
			gd.AsRow("server_go", tags),
			sd.AsRow("server_system", tags),
			md.AsRow("server_memory", tags),
			bd.AsRow("server_build", tags),
			serverRow,
			shardGroupsRow,
			shardsRow,
		}
	*/
}

// Response represents a list of statement results.
type Response struct {
	Results []*influxql.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr.Err != nil {
			return rr.Err
		}
	}
	return nil
}

// Authorize user u to execute query q on database.
// database can be "" for queries that do not require a database.
// If u is nil, this means authorization is disabled.
func (s *Server) Authorize(u *meta.UserInfo, q *influxql.Query, database string) error {
	const authErrLogFmt = "unauthorized request | user: %q | query: %q | database %q\n"

	if u == nil {
		s.Logger.Printf(authErrLogFmt, "", q.String(), database)
		return ErrAuthorize{text: "no user provided"}
	}

	// Cluster admins can do anything.
	if u.Admin {
		return nil
	}

	// Check each statement in the query.
	for _, stmt := range q.Statements {
		// Get the privileges required to execute the statement.
		privs := stmt.RequiredPrivileges()

		// Make sure the user has each privilege required to execute
		// the statement.
		for _, p := range privs {
			// Use the db name specified by the statement or the db
			// name passed by the caller if one wasn't specified by
			// the statement.
			dbname := p.Name
			if dbname == "" {
				dbname = database
			}

			// Check if user has required privilege.
			if !u.Authorize(p.Privilege, dbname) {
				var msg string
				if dbname == "" {
					msg = "requires cluster admin"
				} else {
					msg = fmt.Sprintf("requires %s privilege on %s", p.Privilege.String(), dbname)
				}
				s.Logger.Printf(authErrLogFmt, u.Name, q.String(), database)
				return ErrAuthorize{
					text: fmt.Sprintf("%s not authorized to execute '%s'.  %s", u.Name, stmt.String(), msg),
				}
			}
		}
	}
	return nil
}

// Matcher can match either a Regex or plain string.
type Matcher struct {
	IsRegex bool
	Name    string
}

// Matches returns true of the name passed in matches this Matcher.
func (m *Matcher) Matches(name string) bool {
	if m.IsRegex {
		matches, _ := regexp.MatchString(m.Name, name)
		return matches
	}
	return m.Name == name
}

/*
// ContinuousQuery represents a query that exists on the server and processes
// each incoming event.
type ContinuousQuery struct {
	Query string `json:"query"`

	mu      sync.Mutex
	cq      *influxql.CreateContinuousQueryStatement
	lastRun time.Time
}

func (cq *ContinuousQuery) intoDB() string          { return cq.cq.Source.Target.Measurement.Database }
func (cq *ContinuousQuery) intoRP() string          { return cq.cq.Source.Target.Measurement.RetentionPolicy }
func (cq *ContinuousQuery) setIntoRP(rp string)     { cq.cq.Source.Target.Measurement.RetentionPolicy = rp }
func (cq *ContinuousQuery) intoMeasurement() string { return cq.cq.Source.Target.Measurement.Name }

// NewContinuousQuery returns a ContinuousQuery object with a parsed influxql.CreateContinuousQueryStatement
func NewContinuousQuery(q string) (*ContinuousQuery, error) {
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		return nil, err
	}

	cq, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok {
		return nil, errors.New("query isn't a valid continuous query")
	}

	cquery := &ContinuousQuery{
		Query: q,
		cq:    cq,
	}

	return cquery, nil
}

// shouldRunContinuousQuery returns true if the CQ should be schedule to run. It will use the
// lastRunTime of the CQ and the rules for when to run set through the config to determine
// if this CQ should be run
func (cq *ContinuousQuery) shouldRunContinuousQuery(runsPerInterval int, noMoreThan time.Duration) bool {
	// if it's not aggregated we don't run it
	if cq.cq.Source.IsRawQuery {
		return false
	}

	// since it's aggregated we need to figure how often it should be run
	interval, err := cq.cq.Source.GroupByInterval()
	if err != nil {
		return false
	}

	// determine how often we should run this continuous query.
	// group by time / the number of times to compute
	computeEvery := time.Duration(interval.Nanoseconds()/int64(runsPerInterval)) * time.Nanosecond
	// make sure we're running no more frequently than the setting in the config
	if computeEvery < noMoreThan {
		computeEvery = noMoreThan
	}

	// if we've passed the amount of time since the last run, do it up
	if cq.lastRun.Add(computeEvery).UnixNano() <= time.Now().UnixNano() {
		return true
	}

	return false
}
*/

// RunContinuousQueries will run any continuous queries that are due to run and write the
// results back into the database
func (s *Server) RunContinuousQueries() error {
	panic("not yet implemented")
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()

		for _, d := range s.databases {
			for _, c := range d.continuousQueries {
				// set the into retention policy based on what is now the default
				if c.intoRP() == "" {
					c.setIntoRP(d.defaultRetentionPolicy)
				}
				go func(cq *ContinuousQuery) {
					s.runContinuousQuery(cq)
				}(c)
			}
		}

		return nil
	*/
}

// runContinuousQuery will execute a continuous query
// TODO: make this fan out to the cluster instead of running all the queries on this single data node
/*
func (s *Server) runContinuousQuery(cq *ContinuousQuery) {
	s.stats.Inc("continuousQueryExecuted")
	cq.mu.Lock()
	defer cq.mu.Unlock()

	if !cq.shouldRunContinuousQuery(s.ComputeRunsPerInterval, s.ComputeNoMoreThan) {
		return
	}

	now := time.Now()
	cq.lastRun = now

	interval, err := cq.cq.Source.GroupByInterval()
	if err != nil || interval == 0 {
		return
	}

	startTime := now.Round(interval)
	if startTime.UnixNano() > now.UnixNano() {
		startTime = startTime.Add(-interval)
	}

	if err := cq.cq.Source.SetTimeRange(startTime, startTime.Add(interval)); err != nil {
		log.Printf("cq error setting time range: %s\n", err.Error())
	}

	if err := s.runContinuousQueryAndWriteResult(cq); err != nil {
		log.Printf("cq error: %s. running: %s\n", err.Error(), cq.cq.String())
	}

	for i := 0; i < s.RecomputePreviousN; i++ {
		// if we're already more time past the previous window than we're going to look back, stop
		if now.Sub(startTime) > s.RecomputeNoOlderThan {
			return
		}
		newStartTime := startTime.Add(-interval)

		if err := cq.cq.Source.SetTimeRange(newStartTime, startTime); err != nil {
			log.Printf("cq error setting time range: %s\n", err.Error())
		}

		if err := s.runContinuousQueryAndWriteResult(cq); err != nil {
			log.Printf("cq error during recompute previous: %s. running: %s\n", err.Error(), cq.cq.String())
		}

		startTime = newStartTime
	}
}
*/

// runContinuousQueryAndWriteResult will run the query against the cluster and write the results back in
/*
func (s *Server) runContinuousQueryAndWriteResult(cq *ContinuousQuery) error {
	e, err := s.planSelectStatement(cq.cq.Source, NoChunkingSize)

	if err != nil {
		return err
	}

	// Execute plan.
	ch := e.Execute()

	// Read all rows from channel and write them in
	for row := range ch {
		points, err := s.convertRowToPoints(cq.intoMeasurement(), row)
		if err != nil {
			log.Println(err)
			continue
		}

		if len(points) > 0 {
			for _, p := range points {
				for _, v := range p.Fields() {
					if v == nil {
						// If we have any nil values, we can't write the data
						// This happens the CQ is created and running before we write data to the measurement
						return nil
					}
				}
			}
			_, err = s.WriteSeries(cq.intoDB(), cq.intoRP(), points)
			if err != nil {
				log.Printf("[cq] err: %s", err)
			}
		}
	}

	return nil
}
*/

// convertRowToPoints will convert a query result Row into Points that can be written back in.
// Used for continuous and INTO queries
func (s *Server) convertRowToPoints(measurementName string, row *influxql.Row) ([]tsdb.Point, error) {
	// figure out which parts of the result are the time and which are the fields
	timeIndex := -1
	fieldIndexes := make(map[string]int)
	for i, c := range row.Columns {
		if c == "time" {
			timeIndex = i
		} else {
			fieldIndexes[c] = i
		}
	}

	if timeIndex == -1 {
		return nil, errors.New("cq error finding time index in result")
	}

	points := make([]tsdb.Point, 0, len(row.Values))
	for _, v := range row.Values {
		vals := make(map[string]interface{})
		for fieldName, fieldIndex := range fieldIndexes {
			vals[fieldName] = v[fieldIndex]
		}

		p := tsdb.NewPoint(measurementName, row.Tags, vals, v[timeIndex].(time.Time))

		points = append(points, p)
	}

	return points, nil
}

// StartReportingLoop starts the anonymous usage reporting loop for a given
// cluster ID.
func (s *Server) StartReportingLoop(clusterID uint64) chan struct{} {
	s.reportServer(clusterID)

	ticker := time.NewTicker(24 * time.Hour)
	for {
		select {
		case <-ticker.C:
			s.reportServer(clusterID)
		}
	}
}

func (s *Server) reportServer(clusterID uint64) {
	panic("not yet implemented")
	/*
			s.mu.RLock()
			defer s.mu.RUnlock()

			numSeries, numMeasurements := 0, 0

			for _, db := range s.databases {
				numSeries += len(db.series)
				numMeasurements += len(db.measurements)
			}

			numDatabases := len(s.databases)

			json := fmt.Sprintf(`[{
		    "name":"reports",
		    "columns":["os", "arch", "version", "server_id", "id", "num_series", "num_measurements", "num_databases"],
		    "points":[["%s", "%s", "%s", "%x", "%x", "%d", "%d", "%d"]]
		  }]`, runtime.GOOS, runtime.GOARCH, s.Version, s.ID(), clusterID, numSeries, numMeasurements, numDatabases)

			data := bytes.NewBufferString(json)

			log.Printf("Sending anonymous usage statistics to m.influxdb.com")

			client := http.Client{Timeout: time.Duration(5 * time.Second)}
			go client.Post("http://m.influxdb.com:8086/db/reporting/series?u=reporter&p=influxdb", "application/json", data)
	*/
}

// CreateSnapshotWriter returns a writer for the current snapshot.
// func (s *Server) CreateSnapshotWriter() (*SnapshotWriter, error) {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
// 	return createServerSnapshotWriter(s)
// }
