package influxdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/messaging"
	"golang.org/x/crypto/bcrypt"
)

const (
	// DefaultRootPassword is the password initially set for the root user.
	// It is also used when reseting the root user's password.
	DefaultRootPassword = "root"

	// DefaultRetentionPolicyName is the name of a databases's default shard space.
	DefaultRetentionPolicyName = "default"

	// DefaultSplitN represents the number of partitions a shard is split into.
	DefaultSplitN = 1

	// DefaultReplicaN represents the number of replicas data is written to.
	DefaultReplicaN = 1

	// DefaultShardDuration is the time period held by a shard.
	DefaultShardDuration = 7 * (24 * time.Hour)

	// DefaultShardRetention is the length of time before a shard is dropped.
	DefaultShardRetention = 7 * (24 * time.Hour)

	// BroadcastTopicID is the topic used for all metadata.
	BroadcastTopicID = uint64(0)

	// Defines the minimum duration allowed for all retention policies
	retentionPolicyMinDuration = time.Hour

	// When planning a select statement, passing zero tells it not to chunk results. Only applies to raw queries
	NoChunkingSize = 0
)

// Server represents a collection of metadata and raw metric data.
type Server struct {
	mu       sync.RWMutex
	id       uint64
	path     string
	done     chan struct{} // goroutine close notification
	rpDone   chan struct{} // retention policies goroutine close notification
	sgpcDone chan struct{} // shard group pre-create goroutine close notification

	client MessagingClient  // broker client
	index  uint64           // highest broadcast index seen
	errors map[uint64]error // message errors

	meta *metastore // metadata store

	dataNodes map[uint64]*DataNode // data nodes by id
	databases map[string]*database // databases by name
	users     map[string]*User     // user by name

	shards map[uint64]*Shard // shards by shard id

	stats      *Stats
	Logger     *log.Logger
	WriteTrace bool // Detailed logging of write path

	authenticationEnabled bool

	// Retention policy settings
	RetentionAutoCreate bool

	// continuous query settings
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
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	s := Server{
		meta:      &metastore{},
		errors:    make(map[uint64]error),
		dataNodes: make(map[uint64]*DataNode),
		databases: make(map[string]*database),
		users:     make(map[string]*User),

		shards: make(map[uint64]*Shard),
		stats:  NewStats("server"),
		Logger: log.New(os.Stderr, "[server] ", log.LstdFlags),
	}
	// Server will always return with authentication enabled.
	// This ensures that disabling authentication must be an explicit decision.
	// To set the server to 'authless mode', call server.SetAuthenticationEnabled(false).
	s.authenticationEnabled = true
	return &s
}

func (s *Server) BrokerURLs() []url.URL {
	return s.client.URLs()
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

// Index returns the index for the server.
func (s *Server) Index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

// Path returns the path used when opening the server.
// Returns an empty string when the server is closed.
func (s *Server) Path() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.path
}

// shardPath returns the path for a shard.
func (s *Server) shardPath(id uint64) string {
	if s.path == "" {
		return ""
	}
	return filepath.Join(s.path, "shards", strconv.FormatUint(id, 10))
}

// metaPath returns the path for the metastore.
func (s *Server) metaPath() string {
	if s.path == "" {
		return ""
	}
	return filepath.Join(s.path, "meta")
}

// Open initializes the server from a given path.
func (s *Server) Open(path string, client MessagingClient) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure the server isn't already open and there's a path provided.
	if s.opened() {
		return ErrServerOpen
	} else if path == "" {
		return ErrPathRequired
	}

	// Set the server path.
	s.path = path

	// Create required directories.
	if err := os.MkdirAll(path, 0755); err != nil {
		_ = s.close()
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "shards"), 0755); err != nil {
		_ = s.close()
		return err
	}

	// Set the messaging client.
	s.client = client

	// Open metadata store.
	if err := s.meta.open(s.metaPath()); err != nil {
		_ = s.close()
		return fmt.Errorf("meta: %s", err)
	}

	// Load state from metastore.
	if err := s.load(); err != nil {
		_ = s.close()
		return fmt.Errorf("load: %s", err)
	}

	// Create connection for broadcast topic.
	conn := client.Conn(BroadcastTopicID)
	if err := conn.Open(s.index, true); err != nil {
		_ = s.close()
		return fmt.Errorf("open conn: %s", err)
	}

	// Begin streaming messages from broadcast topic.
	done := make(chan struct{}, 0)
	s.done = done
	go s.processor(conn, done)

	// TODO: Associate series ids with shards.

	return nil
}

// opened returns true when the server is open. Must be called under lock.
func (s *Server) opened() bool { return s.path != "" }

// Close shuts down the server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close()
}

func (s *Server) close() error {
	if !s.opened() {
		return ErrServerClosed
	}

	if s.rpDone != nil {
		close(s.rpDone)
		s.rpDone = nil
	}

	if s.sgpcDone != nil {
		close(s.sgpcDone)
		s.sgpcDone = nil
	}

	// Remove path.
	s.path = ""
	s.index = 0

	// Stop broadcast topic processing.
	if s.done != nil {
		close(s.done)
		s.done = nil
	}

	if s.client != nil {
		s.client.Close()
		s.client = nil
	}

	// Close metastore.
	_ = s.meta.close()

	// Close shards.
	for _, sh := range s.shards {
		_ = sh.close()
	}

	// Server is closing, empty maps which should be reloaded on open.
	s.shards = nil
	s.dataNodes = nil
	s.databases = nil
	s.users = nil

	return nil
}

// load reads the state of the server from the metastore.
func (s *Server) load() error {
	return s.meta.view(func(tx *metatx) error {
		// Read server id & index.
		s.id = tx.id()
		s.index = tx.index()

		// Load data nodes.
		s.dataNodes = make(map[uint64]*DataNode)
		for _, node := range tx.dataNodes() {
			s.dataNodes[node.ID] = node
		}

		// Load databases.
		s.databases = make(map[string]*database)
		for _, db := range tx.databases() {
			s.databases[db.name] = db

			// load the index
			log.Printf("Loading metadata index for %s\n", db.name)
			err := s.meta.view(func(tx *metatx) error {
				tx.indexDatabase(db)
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Load shards.
		s.shards = make(map[uint64]*Shard)
		for _, db := range s.databases {
			for _, rp := range db.policies {
				for _, g := range rp.shardGroups {
					for _, sh := range g.Shards {
						// Add to lookups.
						s.shards[sh.ID] = sh

						// Only open shards owned by the server.
						if !sh.HasDataNodeID(s.id) {
							continue
						}

						if err := sh.open(s.shardPath(sh.ID), s.client.Conn(sh.ID)); err != nil {
							return fmt.Errorf("cannot open shard store: id=%d, err=%s", sh.ID, err)
						}
						s.stats.Inc("shardsOpen")
					}
				}
			}
		}

		// Load users.
		s.users = make(map[string]*User)
		for _, u := range tx.users() {
			s.users[u.Name] = u
		}

		return nil
	})
}

// StartSelfMonitoring starts a goroutine which monitors the InfluxDB server
// itself and stores the results in the specified database at a given interval.
func (s *Server) StartSelfMonitoring(database, retention string, interval time.Duration) error {
	if interval == 0 {
		return fmt.Errorf("statistics check interval must be non-zero")
	}

	// Function for local use turns stats into a slice of points
	pointsFromStats := func(st *Stats, tags map[string]string) []Point {

		var points []Point
		now := time.Now()
		st.Walk(func(k string, v int64) {
			point := Point{
				Timestamp: now,
				Name:      st.name + "_" + k,
				Tags:      make(map[string]string),
				Fields:    map[string]interface{}{"value": int(v)},
			}
			// Specifically create a new map.
			for k, v := range tags {
				point.Tags[k] = v
			}
			points = append(points, point)
		})

		return points
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
			for _, sh := range s.shards {
				batch = append(batch, pointsFromStats(sh.stats, tags)...)
			}

			// Server diagnostics.
			for _, row := range s.DiagnosticsAsRows() {
				points, err := s.convertRowToPoints(row.Name, row)
				if err != nil {
					s.Logger.Printf("failed to write diagnostic row for %s: %s", row.Name, err.Error())
					continue
				}
				for _, p := range points {
					p.Tags = map[string]string{"serverID": strconv.FormatUint(s.ID(), 10)}
				}
				batch = append(batch, points...)
			}

			s.WriteSeries(database, retention, batch)
		}
	}()

	return nil
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
	log.Println("shard group pre-create check commencing")

	// For safety, we double the check interval to ensure we have enough time to create all shard groups
	// before they are needed, but as close to needed as possible.
	// This is a complete punt on optimization
	cutoff := time.Now().Add(checkInterval * 2).UTC()

	type group struct {
		Database  string
		Retention string
		ID        uint64
		Timestamp time.Time
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
						groups = append(groups, group{Database: db.name, Retention: rp.Name, ID: g.ID, Timestamp: g.EndTime.Add(1 * time.Nanosecond)})
					}
				}
			}
		}
	}()

	for _, g := range groups {
		if err := s.CreateShardGroupIfNotExists(g.Database, g.Retention, g.Timestamp); err != nil {
			log.Printf("failed to request pre-creation of shard group %d for time %s: %s", g.ID, g.Timestamp, err.Error())
		}
	}
}

// Client retrieves the current messaging client.
func (s *Server) Client() MessagingClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client
}

// broadcast encodes a message as JSON and send it to the broker's broadcast topic.
// This function waits until the message has been processed by the server.
// Returns the broker log index of the message or an error.
func (s *Server) broadcast(typ messaging.MessageType, c interface{}) (uint64, error) {
	s.stats.Inc("broadcastMessageTx")

	// Encode the command.
	data, err := json.Marshal(c)
	if err != nil {
		return 0, err
	}

	// Publish the message.
	m := &messaging.Message{
		Type:    typ,
		TopicID: BroadcastTopicID,
		Data:    data,
	}
	index, err := s.client.Publish(m)
	if err != nil {
		return 0, err
	}

	// Wait for the server to receive the message.
	err = s.Sync(BroadcastTopicID, index)

	return index, err
}

// Sync blocks until a given index (or a higher index) has been applied.
// Returns any error associated with the command.
func (s *Server) Sync(topicID, index uint64) error {
	// Sync to the broadcast topic if specified.
	if topicID == 0 {
		return s.syncBroadcast(index)
	}

	// Otherwise retrieve shard by id.
	s.mu.RLock()
	sh := s.shards[topicID]
	s.mu.RUnlock()

	// Return error if there is no shard.
	if sh == nil || sh.store == nil {
		return errors.New("shard not owned")
	}

	return sh.sync(index)
}

// syncBroadcast syncs the broadcast topic.
func (s *Server) syncBroadcast(index uint64) error {
	for {
		// Check if index has occurred. If so, retrieve the error and return.
		s.mu.RLock()
		if s.index >= index {
			err, ok := s.errors[index]
			if ok {
				delete(s.errors, index)
			}
			s.mu.RUnlock()
			return err
		}
		s.mu.RUnlock()

		// Otherwise wait momentarily and check again.
		time.Sleep(1 * time.Millisecond)
	}
}

// Initialize creates a new data node and initializes the server's id to the latest.
func (s *Server) Initialize(u url.URL) error {
	// Create a new data node.
	if err := s.CreateDataNode(&u); err != nil {
		return err
	}

	// Ensure the data node returns with an ID.
	// If it doesn't then something went really wrong. We have to panic because
	// the messaging client relies on the first server being assigned ID 1.
	n := s.DataNodeByURL(&u)
	assert(n != nil, "node not created: %s", u.String())
	assert(n.ID > 0, "invalid node id: %d", n.ID)

	// Set the ID on the metastore.
	if err := s.meta.mustUpdate(0, func(tx *metatx) error {
		return tx.setID(n.ID)
	}); err != nil {
		return err
	}

	// Set the ID on the server.
	s.id = n.ID

	return nil
}

// This is the same struct we use in the httpd package, but
// it seems overkill to export it and share it
type dataNodeJSON struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

// copyURL returns a copy of the the URL.
func copyURL(u *url.URL) *url.URL {
	other := &url.URL{}
	*other = *u
	return other
}

// Join creates a new data node in an existing cluster, copies the metastore,
// and initializes the ID.
func (s *Server) Join(u *url.URL, joinURL *url.URL) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create the initial request. Might get a redirect though depending on
	// the nodes role
	joinURL = copyURL(joinURL)
	joinURL.Path = "/data_nodes"

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
			retries += 1
			time.Sleep(1 * time.Second)
			continue
		}

		// We likely tried to join onto a broker which cannot handle this request.  It
		// has given us the address of a known data node to join instead.
		if resp.StatusCode == http.StatusTemporaryRedirect {
			redirectURL, err := url.Parse(resp.Header.Get("Location"))

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
	joinURL.Path = "/metastore"
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
}

// CopyMetastore writes the underlying metastore data file to a writer.
func (s *Server) CopyMetastore(w io.Writer) error {
	return s.meta.mustView(func(tx *metatx) error {
		// Set content lengh if this is a HTTP connection.
		if w, ok := w.(http.ResponseWriter); ok {
			w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		}

		// Write entire database to the writer.
		return tx.Copy(w)
	})
}

// DataNode returns a data node by id.
func (s *Server) DataNode(id uint64) *DataNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dataNodes[id]
}

// DataNodesByID returns the data nodes matching the passed ids
func (s *Server) DataNodesByID(ids []uint64) []*DataNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var a []*DataNode
	for _, id := range ids {
		a = append(a, s.dataNodes[id])
	}
	return a
}

// DataNodeByURL returns a data node by url.
func (s *Server) DataNodeByURL(u *url.URL) *DataNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.dataNodes {
		if n.URL.String() == u.String() {
			return n
		}
	}
	return nil
}

// DataNodes returns a list of data nodes.
func (s *Server) DataNodes() (a []*DataNode) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.dataNodes {
		a = append(a, n)
	}
	sort.Sort(dataNodes(a))
	return
}

// CreateDataNode creates a new data node with a given URL.
func (s *Server) CreateDataNode(u *url.URL) error {
	c := &createDataNodeCommand{URL: u.String()}
	_, err := s.broadcast(createDataNodeMessageType, c)
	return err
}

func (s *Server) applyCreateDataNode(m *messaging.Message) (err error) {
	var c createDataNodeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate parameters.
	if c.URL == "" {
		return ErrDataNodeURLRequired
	}

	// Check that another node with the same URL doesn't already exist.
	u, _ := url.Parse(c.URL)
	for _, n := range s.dataNodes {
		if n.URL.String() == u.String() {
			return ErrDataNodeExists
		}
	}

	// Create data node.
	n := newDataNode()
	n.URL = u

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		n.ID = tx.nextDataNodeID()
		return tx.saveDataNode(n)
	})

	// Add to node on server.
	s.dataNodes[n.ID] = n

	return
}

// DeleteDataNode deletes an existing data node.
func (s *Server) DeleteDataNode(id uint64) error {
	c := &deleteDataNodeCommand{ID: id}
	_, err := s.broadcast(deleteDataNodeMessageType, c)
	return err
}

func (s *Server) applyDeleteDataNode(m *messaging.Message) (err error) {
	var c deleteDataNodeCommand
	mustUnmarshalJSON(m.Data, &c)

	n := s.dataNodes[c.ID]
	if n == nil {
		return ErrDataNodeNotFound
	}

	// Remove from metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error { return tx.deleteDataNode(c.ID) })

	// Delete the node.
	delete(s.dataNodes, n.ID)

	return
}

// DatabaseExists returns true if a database exists.
func (s *Server) DatabaseExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.databases[name] != nil
}

// Databases returns a sorted list of all database names.
func (s *Server) Databases() (a []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, db := range s.databases {
		a = append(a, db.name)
	}
	sort.Strings(a)
	return
}

// CreateDatabase creates a new database.
func (s *Server) CreateDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	}
	c := &createDatabaseCommand{Name: name}
	_, err := s.broadcast(createDatabaseMessageType, c)
	return err
}

// CreateDatabaseIfNotExists creates a new database if, and only if, it does not exist already.
func (s *Server) CreateDatabaseIfNotExists(name string) error {
	if s.DatabaseExists(name) {
		return nil
	}

	// Small chance database could have been created even though the check above said it didn't.
	if err := s.CreateDatabase(name); err != nil && err != ErrDatabaseExists {
		return err
	}
	return nil
}

func (s *Server) applyCreateDatabase(m *messaging.Message) (err error) {
	var c createDatabaseCommand
	mustUnmarshalJSON(m.Data, &c)

	if s.databases[c.Name] != nil {
		return ErrDatabaseExists
	}

	// Create database entry.
	db := newDatabase()
	db.name = c.Name

	if s.RetentionAutoCreate {
		// Create the default retention policy.
		db.policies[DefaultRetentionPolicyName] = &RetentionPolicy{
			Name:               DefaultRetentionPolicyName,
			Duration:           0,
			ShardGroupDuration: calculateShardGroupDuration(0),
			ReplicaN:           1,
		}
		db.defaultRetentionPolicy = DefaultRetentionPolicyName
		s.Logger.Printf("retention policy '%s' auto-created for database '%s'", DefaultRetentionPolicyName, c.Name)
	}

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error { return tx.saveDatabase(db) })

	// Add to databases on server.
	s.databases[c.Name] = db

	return
}

// DropDatabase deletes an existing database.
func (s *Server) DropDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	}
	c := &dropDatabaseCommand{Name: name}
	_, err := s.broadcast(dropDatabaseMessageType, c)
	return err
}

func (s *Server) applyDropDatabase(m *messaging.Message) (err error) {
	var c dropDatabaseCommand
	mustUnmarshalJSON(m.Data, &c)

	if s.databases[c.Name] == nil {
		return ErrDatabaseNotFound(c.Name)
	}

	// Remove from metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error { return tx.dropDatabase(c.Name) })

	// Delete the database entry.
	delete(s.databases, c.Name)
	return
}

// Shard returns a shard by ID.
func (s *Server) Shard(id uint64) *Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shards[id]
}

// shardGroupByTimestamp returns a group for a database, policy & timestamp.
func (s *Server) shardGroupByTimestamp(database, policy string, timestamp time.Time) (*ShardGroup, error) {
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}
	return db.shardGroupByTimestamp(policy, timestamp)
}

// ShardGroups returns a list of all shard groups for a database.
// Returns an error if the database doesn't exist.
func (s *Server) ShardGroups(database string) ([]*ShardGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	// Retrieve groups from database.
	var a []*ShardGroup
	for _, rp := range db.policies {
		for _, g := range rp.shardGroups {
			a = append(a, g)
		}
	}
	return a, nil
}

// CreateShardGroupIfNotExists creates the shard group for a retention policy for the interval a timestamp falls into.
func (s *Server) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) error {
	c := &createShardGroupIfNotExistsCommand{Database: database, Policy: policy, Timestamp: timestamp}
	_, err := s.broadcast(createShardGroupIfNotExistsMessageType, c)
	return err
}

func (s *Server) applyCreateShardGroupIfNotExists(m *messaging.Message) (err error) {
	var c createShardGroupIfNotExistsCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	// Validate retention policy.
	rp := db.policies[c.Policy]
	if rp == nil {
		return ErrRetentionPolicyNotFound
	}

	// If we can match to an existing shard group date range then just ignore request.
	if g := rp.shardGroupByTimestamp(c.Timestamp); g != nil {
		return nil
	}

	// If no shards match then create a new one.
	g := newShardGroup()
	g.StartTime = c.Timestamp.Truncate(rp.ShardGroupDuration).UTC()
	g.EndTime = g.StartTime.Add(rp.ShardGroupDuration).UTC()

	// Sort nodes so they're consistently assigned to the shards.
	nodes := make([]*DataNode, 0, len(s.dataNodes))
	for _, n := range s.dataNodes {
		nodes = append(nodes, n)
	}
	sort.Sort(dataNodes(nodes))

	// Require at least one replica but no more replicas than nodes.
	replicaN := int(rp.ReplicaN)
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > len(nodes) {
		replicaN = len(nodes)
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := len(nodes) / replicaN

	// Create a shard based on the node count and replication factor.
	g.Shards = make([]*Shard, shardN)
	for i := range g.Shards {
		g.Shards[i] = newShard()
	}

	// Persist to metastore if a shard was created.
	if err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		// Generate an ID for the group.
		g.ID = tx.nextShardGroupID()

		// Generate an ID for each shard.
		for _, sh := range g.Shards {
			sh.ID = tx.nextShardID()
		}

		// Assign data nodes to shards via round robin.
		// Start from a repeatably "random" place in the node list.
		nodeIndex := int(m.Index % uint64(len(nodes)))
		for _, sh := range g.Shards {
			for i := 0; i < replicaN; i++ {
				node := nodes[nodeIndex%len(nodes)]
				sh.DataNodeIDs = append(sh.DataNodeIDs, node.ID)
				nodeIndex++
			}
		}
		s.stats.Add("shardsCreated", int64(len(g.Shards)))

		// Retention policy has a new shard group, so update the policy.
		rp.shardGroups = append(rp.shardGroups, g)

		return tx.saveDatabase(db)
	}); err != nil {
		g.close()
		return
	}

	// Open shards assigned to this server.
	for _, sh := range g.Shards {
		// Ignore if this server is not assigned.
		if !sh.HasDataNodeID(s.id) {
			continue
		}

		// Open shard store. Panic if an error occurs and we can retry.
		if err := sh.open(s.shardPath(sh.ID), s.client.Conn(sh.ID)); err != nil {
			panic("unable to open shard: " + err.Error())
		}
	}

	// Add to lookups.
	for _, sh := range g.Shards {
		s.shards[sh.ID] = sh
	}

	return
}

// DeleteShardGroup deletes the shard group identified by shardID.
func (s *Server) DeleteShardGroup(database, policy string, shardID uint64) error {
	c := &deleteShardGroupCommand{Database: database, Policy: policy, ID: shardID}
	_, err := s.broadcast(deleteShardGroupMessageType, c)
	return err
}

// applyDeleteShardGroup deletes shard data from disk and updates the metastore.
func (s *Server) applyDeleteShardGroup(m *messaging.Message) (err error) {
	var c deleteShardGroupCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	// Validate retention policy.
	rp := db.policies[c.Policy]
	if rp == nil {
		return ErrRetentionPolicyNotFound
	}

	// If shard group no longer exists, then ignore request. This can occur if multiple
	// data nodes triggered the deletion.
	g := rp.shardGroupByID(c.ID)
	if g == nil {
		return nil
	}

	for _, shard := range g.Shards {
		// Ignore shards not on this server.
		if !shard.HasDataNodeID(s.id) {
			continue
		}

		path := shard.store.Path()
		shard.close()
		if err := os.Remove(path); err != nil {
			// Log, but keep going. This can happen if shards were deleted, but the server exited
			// before it acknowledged the delete command.
			log.Printf("error deleting shard %s, group ID %d, policy %s: %s", path, g.ID, rp.Name, err.Error())
		}
	}

	// Remove from metastore.
	rp.removeShardGroupByID(c.ID)
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		s.stats.Add("shardsDeleted", int64(len(g.Shards)))
		return tx.saveDatabase(db)
	})
	return
}

// User returns a user by username
// Returns nil if the user does not exist.
func (s *Server) User(name string) *User {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.users[name]
}

// Users returns a list of all users, sorted by name.
func (s *Server) Users() (a []*User) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		a = append(a, u)
	}
	sort.Sort(users(a))
	return a
}

// UserCount returns the number of users.
func (s *Server) UserCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users)
}

// AdminUserExists returns whether at least 1 admin-level user exists.
func (s *Server) AdminUserExists() bool {
	for _, u := range s.users {
		if u.Admin {
			return true
		}
	}
	return false
}

// Authenticate returns an authenticated user by username. If any error occurs,
// or the authentication credentials are invalid, an error is returned.
func (s *Server) Authenticate(username, password string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	u := s.users[username]

	// If authorization is not enabled and user is nil, we are authorized
	if u == nil && !s.authenticationEnabled {
		return nil, nil
	}
	if u == nil {
		return nil, fmt.Errorf("invalid username or password")
	}
	err := u.Authenticate(password)
	if err != nil {
		return nil, fmt.Errorf("invalid username or password")
	}
	return u, nil
}

// CreateUser creates a user on the server.
func (s *Server) CreateUser(username, password string, admin bool) error {
	c := &createUserCommand{Username: username, Password: password, Admin: admin}
	_, err := s.broadcast(createUserMessageType, c)
	return err
}

func (s *Server) applyCreateUser(m *messaging.Message) (err error) {
	var c createUserCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.users[c.Username] != nil {
		return ErrUserExists
	}

	// Generate the hash of the password.
	hash, err := HashPassword(c.Password)
	if err != nil {
		return err
	}

	// Create the user.
	u := &User{
		Name:       c.Username,
		Hash:       string(hash),
		Privileges: make(map[string]influxql.Privilege),
		Admin:      c.Admin,
	}

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveUser(u)
	})

	s.users[u.Name] = u
	return
}

// UpdateUser updates an existing user on the server.
func (s *Server) UpdateUser(username, password string) error {
	c := &updateUserCommand{Username: username, Password: password}
	_, err := s.broadcast(updateUserMessageType, c)
	return err
}

func (s *Server) applyUpdateUser(m *messaging.Message) (err error) {
	var c updateUserCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate command.
	u := s.users[c.Username]
	if u == nil {
		return ErrUserNotFound
	}

	// Update the user's password, if set.
	if c.Password != "" {
		hash, err := HashPassword(c.Password)
		if err != nil {
			return err
		}
		u.Hash = string(hash)
	}

	// Persist to metastore.
	return s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveUser(u)
	})
}

// DeleteUser removes a user from the server.
func (s *Server) DeleteUser(username string) error {
	c := &deleteUserCommand{Username: username}
	_, err := s.broadcast(deleteUserMessageType, c)
	return err
}

func (s *Server) applyDeleteUser(m *messaging.Message) error {
	var c deleteUserCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	} else if s.users[c.Username] == nil {
		return ErrUserNotFound
	}

	// Remove from metastore.
	s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.deleteUser(c.Username)
	})

	// Delete the user.
	delete(s.users, c.Username)
	return nil
}

// SetPrivilege grants / revokes a privilege to a user.
func (s *Server) SetPrivilege(p influxql.Privilege, username string, dbname string) error {
	c := &setPrivilegeCommand{p, username, dbname}
	_, err := s.broadcast(setPrivilegeMessageType, c)
	return err
}

func (s *Server) applySetPrivilege(m *messaging.Message) error {
	var c setPrivilegeCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate user.
	if c.Username == "" {
		return ErrUsernameRequired
	}

	u := s.users[c.Username]
	if u == nil {
		return ErrUserNotFound
	}

	// If dbname is empty, update user's Admin flag.
	if c.Database == "" && (c.Privilege == influxql.AllPrivileges || c.Privilege == influxql.NoPrivileges) {
		u.Admin = (c.Privilege == influxql.AllPrivileges)
	} else if c.Database != "" {
		// Update user's privilege for the database.
		u.Privileges[c.Database] = c.Privilege
	} else {
		return ErrInvalidGrantRevoke
	}

	// Persist to metastore.
	return s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveUser(u)
	})
}

// RetentionPolicy returns a retention policy by name.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicy(database, name string) (*RetentionPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	return db.policies[name], nil
}

// DefaultRetentionPolicy returns the default retention policy for a database.
// Returns an error if the database doesn't exist.
func (s *Server) DefaultRetentionPolicy(database string) (*RetentionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	return db.policies[db.defaultRetentionPolicy], nil
}

// RetentionPolicies returns a list of retention polocies for a database.
// Returns an error if the database doesn't exist.
func (s *Server) RetentionPolicies(database string) ([]*RetentionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lookup database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	// Retrieve the policies.
	a := make(RetentionPolicies, 0, len(db.policies))
	for _, p := range db.policies {
		a = append(a, p)
	}
	sort.Sort(a)
	return a, nil
}

// RetentionPolicyExists returns true if a retention policy exists for a given database.
func (s *Server) RetentionPolicyExists(database, retention string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DatabaseExists(database) && s.databases[database].policies[retention] != nil
}

// CreateRetentionPolicy creates a retention policy for a database.
func (s *Server) CreateRetentionPolicy(database string, rp *RetentionPolicy) error {
	// Enforce duration of at least retentionPolicyMinDuration
	if rp.Duration < retentionPolicyMinDuration && rp.Duration != 0 {
		return ErrRetentionPolicyMinDuration
	}

	c := &createRetentionPolicyCommand{
		Database:           database,
		Name:               rp.Name,
		Duration:           rp.Duration,
		ShardGroupDuration: calculateShardGroupDuration(rp.Duration),
		ReplicaN:           rp.ReplicaN,
	}
	_, err := s.broadcast(createRetentionPolicyMessageType, c)
	return err
}

// CreateRetentionPolicyIfNotExists creates a retention policy for a database.
func (s *Server) CreateRetentionPolicyIfNotExists(database string, rp *RetentionPolicy) error {
	// Ensure retention policy exists.
	if !s.RetentionPolicyExists(database, rp.Name) {
		// Small chance retention policy could be created after it didn't exist when checked.
		if err := s.CreateRetentionPolicy(database, rp); err != nil && err != ErrRetentionPolicyExists {
			return err
		}
	}
	return nil
}

func calculateShardGroupDuration(d time.Duration) time.Duration {
	const (
		day   = time.Hour * 24
		month = day * 30
	)

	switch {
	case d > 6*month || d == 0:
		return 7 * day
	case d > 2*day:
		return 1 * day
	default:
		return 1 * time.Hour
	}
}

func (s *Server) applyCreateRetentionPolicy(m *messaging.Message) error {
	var c createRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[c.Name] != nil {
		return ErrRetentionPolicyExists
	}

	// Add policy to the database.
	db.policies[c.Name] = &RetentionPolicy{
		Name:               c.Name,
		Duration:           c.Duration,
		ShardGroupDuration: c.ShardGroupDuration,
		ReplicaN:           c.ReplicaN,
	}

	// Persist to metastore.
	s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

// RetentionPolicyUpdate represents retention policy fields that
// need to be updated.
type RetentionPolicyUpdate struct {
	Name     *string        `json:"name,omitempty"`
	Duration *time.Duration `json:"duration,omitempty"`
	ReplicaN *uint32        `json:"replicaN,omitempty"`
}

// UpdateRetentionPolicy updates an existing retention policy on a database.
func (s *Server) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error {
	// Enforce duration of at least retentionPolicyMinDuration
	if rpu.Duration != nil && *rpu.Duration < retentionPolicyMinDuration && *rpu.Duration != 0 {
		return ErrRetentionPolicyMinDuration
	}

	c := &updateRetentionPolicyCommand{Database: database, Name: name, Policy: rpu}
	_, err := s.broadcast(updateRetentionPolicyMessageType, c)
	return err
}

func (s *Server) applyUpdateRetentionPolicy(m *messaging.Message) (err error) {
	var c updateRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate command.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	}

	// Retrieve the policy.
	p := db.policies[c.Name]
	if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Update the policy name.
	if c.Policy.Name != nil {
		delete(db.policies, p.Name)
		p.Name = *c.Policy.Name
		db.policies[p.Name] = p
	}

	// Update duration.
	if c.Policy.Duration != nil {
		p.Duration = *c.Policy.Duration
	}

	// Update replication factor.
	if c.Policy.ReplicaN != nil {
		p.ReplicaN = *c.Policy.ReplicaN
	}

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

// DeleteRetentionPolicy removes a retention policy from a database.
func (s *Server) DeleteRetentionPolicy(database, name string) error {
	c := &deleteRetentionPolicyCommand{Database: database, Name: name}
	_, err := s.broadcast(deleteRetentionPolicyMessageType, c)
	return err
}

func (s *Server) applyDeleteRetentionPolicy(m *messaging.Message) (err error) {
	var c deleteRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	// Retrieve the database.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	} else if c.Name == "" {
		return ErrRetentionPolicyNameRequired
	} else if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Remove retention policy.
	delete(db.policies, c.Name)

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

// SetDefaultRetentionPolicy sets the default policy to write data into and query from on a database.
func (s *Server) SetDefaultRetentionPolicy(database, name string) error {
	c := &setDefaultRetentionPolicyCommand{Database: database, Name: name}
	_, err := s.broadcast(setDefaultRetentionPolicyMessageType, c)
	return err
}

func (s *Server) applySetDefaultRetentionPolicy(m *messaging.Message) (err error) {
	var c setDefaultRetentionPolicyCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate command.
	db := s.databases[c.Database]
	if s.databases[c.Database] == nil {
		return ErrDatabaseNotFound(c.Database)
	} else if db.policies[c.Name] == nil {
		return ErrRetentionPolicyNotFound
	}

	// Update default policy.
	db.defaultRetentionPolicy = c.Name

	// Persist to metastore.
	err = s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return
}

func (s *Server) applyDropSeries(m *messaging.Message) error {
	var c dropSeriesCommand
	mustUnmarshalJSON(m.Data, &c)

	database := s.databases[c.Database]
	if database == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	// Remove from metastore.
	err := s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		if err := tx.dropSeries(c.Database, c.SeriesByMeasurement); err != nil {
			return err
		}

		// Delete series from the database.
		if err := database.dropSeries(c.SeriesByMeasurement); err != nil {
			return fmt.Errorf("failed to remove series from index: %s", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// DropSeries deletes from an existing series.
func (s *Server) DropSeries(database string, seriesByMeasurement map[string][]uint64) error {
	c := dropSeriesCommand{Database: database, SeriesByMeasurement: seriesByMeasurement}
	_, err := s.broadcast(dropSeriesMessageType, c)
	return err
}

// Point defines the values that will be written to the database
type Point struct {
	Name      string
	Tags      map[string]string
	Timestamp time.Time
	Fields    map[string]interface{}
}

// WriteSeries writes series data to the database.
// Returns the messaging index the data was written to.
func (s *Server) WriteSeries(database, retentionPolicy string, points []Point) (idx uint64, err error) {
	s.stats.Inc("batchWriteRx")
	s.stats.Add("pointWriteRx", int64(len(points)))
	defer func() {
		if err != nil {
			s.stats.Inc("batchWriteRxError")
		}
	}()

	if s.WriteTrace {
		log.Printf("received write for database '%s', retention policy '%s', with %d points",
			database, retentionPolicy, len(points))
	}

	// Make sure every point has at least one field.
	for _, p := range points {
		if len(p.Fields) == 0 {
			return 0, ErrFieldsRequired
		}
	}

	// If the retention policy is not set, use the default for this database.
	if retentionPolicy == "" {
		rp, err := s.DefaultRetentionPolicy(database)
		if err != nil {
			return 0, fmt.Errorf("failed to determine default retention policy: %s", err.Error())
		} else if rp == nil {
			return 0, ErrDefaultRetentionPolicyNotFound
		}
		retentionPolicy = rp.Name
	}

	// Ensure all required Series and Measurement Fields are created cluster-wide.
	if err := s.createMeasurementsIfNotExists(database, retentionPolicy, points); err != nil {
		return 0, err
	}
	if s.WriteTrace {
		log.Printf("measurements and series created on database '%s'", database)
	}

	// Ensure all the required shard groups exist. TODO: this should be done async.
	if err := s.createShardGroupsIfNotExists(database, retentionPolicy, points); err != nil {
		return 0, err
	}
	if s.WriteTrace {
		log.Printf("shard groups created for database '%s'", database)
	}

	// Build writeRawSeriesMessageType publish commands.
	shardData := make(map[uint64][]byte, 0)
	codecs := make(map[string]*FieldCodec, 0)
	if err := func() error {
		// Local function makes lock management foolproof.
		s.mu.RLock()
		defer s.mu.RUnlock()

		db := s.databases[database]
		if db == nil {
			return ErrDatabaseNotFound(database)
		}
		for _, p := range points {
			measurement, series := db.MeasurementAndSeries(p.Name, p.Tags)
			if series == nil {
				s.Logger.Printf("series not found: name=%s, tags=%#v", p.Name, p.Tags)
				return ErrSeriesNotFound
			}

			// Retrieve shard group.
			g, err := s.shardGroupByTimestamp(database, retentionPolicy, p.Timestamp)
			if err != nil {
				return err
			}
			if s.WriteTrace {
				log.Printf("shard group located: %v", g)
			}

			// Find appropriate shard within the shard group.
			sh := g.ShardBySeriesID(series.ID)
			if s.WriteTrace {
				log.Printf("shard located: %v", sh)
			}

			// Many points are likely to have the same Measurement name. Re-use codecs if possible.
			var codec *FieldCodec
			codec, ok := codecs[measurement.Name]
			if !ok {
				codec = NewFieldCodec(measurement)
				codecs[measurement.Name] = codec
			}

			// Convert string-key/values to encoded fields.
			encodedFields, err := codec.EncodeFields(p.Fields)
			if err != nil {
				return err
			}

			// Encode point header, followed by point data, and add to shard's batch.
			data := marshalPointHeader(series.ID, uint32(len(encodedFields)), p.Timestamp.UnixNano())
			data = append(data, encodedFields...)
			if shardData[sh.ID] == nil {
				shardData[sh.ID] = make([]byte, 0)
			}
			shardData[sh.ID] = append(shardData[sh.ID], data...)
			if s.WriteTrace {
				log.Printf("data appended to buffer for shard %d", sh.ID)
			}
		}

		return nil
	}(); err != nil {
		return 0, err
	}

	// Write data for each shard to the Broker.
	var maxIndex uint64
	for i, d := range shardData {
		assert(len(d) > 0, "raw series data required: topic=%d", i)

		index, err := s.client.Publish(&messaging.Message{
			Type:    writeRawSeriesMessageType,
			TopicID: i,
			Data:    d,
		})
		if err != nil {
			return maxIndex, err
		}
		s.stats.Inc("writeSeriesMessageTx")
		if index > maxIndex {
			maxIndex = index
		}
		if s.WriteTrace {
			log.Printf("write series message published successfully for topic %d", i)
		}
	}

	return maxIndex, nil
}

// createMeasurementsIfNotExists walks the "points" and ensures that all new Series are created, and all
// new Measurement fields have been created, across the cluster.
func (s *Server) createMeasurementsIfNotExists(database, retentionPolicy string, points []Point) error {
	c := newCreateMeasurementsIfNotExistsCommand(database)

	// Local function keeps lock management foolproof.
	func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()

		db := s.databases[database]
		if db == nil {
			return ErrDatabaseNotFound(database)
		}

		for _, p := range points {
			measurement, series := db.MeasurementAndSeries(p.Name, p.Tags)

			if series == nil {
				// Series does not exist in Metastore, add it so it's created cluster-wide.
				c.addSeriesIfNotExists(p.Name, p.Tags)
			}

			for k, v := range p.Fields {
				if measurement != nil {
					if f := measurement.FieldByName(k); f != nil {
						// Field present in Metastore, make sure there is no type conflict.
						if f.Type != influxql.InspectDataType(v) {
							return fmt.Errorf("field \"%s\" is type %T, mapped as type %s", k, v, f.Type)
						}
						continue // Field is present, and it's of the same type. Nothing more to do.
					}
				}
				// Field isn't in Metastore. Add it to command so it's created cluster-wide.
				if err := c.addFieldIfNotExists(p.Name, k, influxql.InspectDataType(v)); err != nil {
					return err
				}
			}
		}

		return nil
	}()

	// Any broadcast actually required?
	if len(c.Measurements) > 0 {
		_, err := s.broadcast(createMeasurementsIfNotExistsMessageType, c)
		if err != nil {
			return err
		}
	}

	return nil
}

// applyCreateMeasurementsIfNotExists creates the Measurements, Series, and Fields in the Metastore.
func (s *Server) applyCreateMeasurementsIfNotExists(m *messaging.Message) error {
	var c createMeasurementsIfNotExistsCommand
	mustUnmarshalJSON(m.Data, &c)

	// Validate command.
	db := s.databases[c.Database]
	if db == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	// Process command within a transaction.
	if err := s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		for _, cm := range c.Measurements {
			// Create each series
			for _, t := range cm.Tags {
				_, ss := db.MeasurementAndSeries(cm.Name, t)

				// Ensure creation of Series is idempotent.
				if ss != nil {
					continue
				}

				series, err := tx.createSeries(db.name, cm.Name, t)
				if err != nil {
					return err
				}
				db.addSeriesToIndex(cm.Name, series)
			}

			// Create each new field.
			mm := db.measurements[cm.Name]
			if mm == nil {
				panic(fmt.Sprintf("measurement not found: %s", cm.Name))
			}
			for _, f := range cm.Fields {
				if err := mm.createFieldIfNotExists(f.Name, f.Type); err != nil {
					if err == ErrFieldOverflow {
						log.Printf("no more fields allowed: %s::%s", mm.Name, f.Name)
						continue
					} else if err == ErrFieldTypeConflict {
						log.Printf("field type conflict: %s::%s", mm.Name, f.Name)
						continue
					}
					return err
				}
				if err := tx.saveMeasurement(db.name, mm); err != nil {
					return fmt.Errorf("save measurement: %s", err)
				}
			}
			if err := tx.saveDatabase(db); err != nil {
				return fmt.Errorf("save database: %s", err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// DropMeasurement drops a given measurement from a database.
func (s *Server) DropMeasurement(database, name string) error {
	c := &dropMeasurementCommand{Database: database, Name: name}
	_, err := s.broadcast(dropMeasurementMessageType, c)
	return err
}

func (s *Server) applyDropMeasurement(m *messaging.Message) error {
	var c dropMeasurementCommand
	mustUnmarshalJSON(m.Data, &c)

	database := s.databases[c.Database]
	if database == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	measurement := database.measurements[c.Name]
	if measurement == nil {
		return ErrMeasurementNotFound(c.Name)
	}

	err := s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		// Drop metastore data
		if err := tx.dropMeasurement(c.Database, c.Name); err != nil {
			return err
		}

		// Drop measurement from the database.
		if err := database.dropMeasurement(c.Name); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// createShardGroupsIfNotExist walks the "points" and ensures that all required shards exist on the cluster.
func (s *Server) createShardGroupsIfNotExists(database, retentionPolicy string, points []Point) error {
	for _, p := range points {
		// Check if shard group exists first.
		g, err := s.shardGroupByTimestamp(database, retentionPolicy, p.Timestamp)
		if err != nil {
			return err
		} else if g != nil {
			continue
		}
		err = s.CreateShardGroupIfNotExists(database, retentionPolicy, p.Timestamp)
		if err != nil {
			return fmt.Errorf("create shard(%s/%s): %s", retentionPolicy, p.Timestamp.Format(time.RFC3339Nano), err)
		}
	}

	return nil
}

// ReadSeries reads a single point from a series in the database. It is used for debug and test only.
func (s *Server) ReadSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find database.
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	// Find series.
	mm, series := db.MeasurementAndSeries(name, tags)
	if mm == nil {
		return nil, ErrMeasurementNotFound("")
	} else if series == nil {
		return nil, ErrSeriesNotFound
	}

	// If the retention policy is not specified, use the default for this database.
	if retentionPolicy == "" {
		retentionPolicy = db.defaultRetentionPolicy
	}

	// Retrieve retention policy.
	rp := db.policies[retentionPolicy]
	if rp == nil {
		return nil, ErrRetentionPolicyNotFound
	}

	// Retrieve shard group.
	g, err := s.shardGroupByTimestamp(database, retentionPolicy, timestamp)
	if err != nil {
		return nil, err
	} else if g == nil {
		return nil, nil
	}

	// TODO: Verify that server owns shard.

	// Find appropriate shard within the shard group.
	sh := g.Shards[int(series.ID)%len(g.Shards)]

	// Read raw encoded series data.
	data, err := sh.readSeries(series.ID, timestamp.UnixNano())
	if err != nil {
		return nil, err
	}

	// Decode into a raw value map.
	codec := NewFieldCodec(mm)
	rawFields, err := codec.DecodeFields(data)
	if err != nil || rawFields == nil {
		return nil, nil
	}

	// Decode into a string-key value map.
	values := make(map[string]interface{}, len(rawFields))
	for fieldID, value := range rawFields {
		f := mm.Field(fieldID)
		if f == nil {
			continue
		}
		values[f.Name] = value
	}

	return values, nil
}

// ExecuteQuery executes an InfluxQL query against the server.
// If the user isn't authorized to access the database an error will be returned.
// It sends results down the passed in chan and closes it when done. It will close the chan
// on the first statement that throws an error.
func (s *Server) ExecuteQuery(q *influxql.Query, database string, user *User, chunkSize int) (chan *Result, error) {
	// Authorize user to execute the query.
	if s.authenticationEnabled {
		if err := s.Authorize(user, q, database); err != nil {
			return nil, err
		}
	}

	s.stats.Add("queriesRx", int64(len(q.Statements)))

	// Execute each statement. Keep the iterator external so we can
	// track how many of the statements were executed
	results := make(chan *Result)
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
					results <- &Result{Err: err}
					break
				}
			}

			var res *Result
			switch stmt := stmt.(type) {
			case *influxql.SelectStatement:
				if err := s.executeSelectStatement(i, stmt, database, user, results, chunkSize); err != nil {
					results <- &Result{Err: err}
					break
				}
			case *influxql.CreateDatabaseStatement:
				res = s.executeCreateDatabaseStatement(stmt, user)
			case *influxql.DropDatabaseStatement:
				res = s.executeDropDatabaseStatement(stmt, user)
			case *influxql.ShowDatabasesStatement:
				res = s.executeShowDatabasesStatement(stmt, user)
			case *influxql.ShowServersStatement:
				res = s.executeShowServersStatement(stmt, user)
			case *influxql.CreateUserStatement:
				res = s.executeCreateUserStatement(stmt, user)
			case *influxql.SetPasswordUserStatement:
				res = s.executeSetPasswordUserStatement(stmt, user)
			case *influxql.DeleteStatement:
				res = s.executeDeleteStatement()
			case *influxql.DropUserStatement:
				res = s.executeDropUserStatement(stmt, user)
			case *influxql.ShowUsersStatement:
				res = s.executeShowUsersStatement(stmt, user)
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
			case *influxql.GrantStatement:
				res = s.executeGrantStatement(stmt, user)
			case *influxql.RevokeStatement:
				res = s.executeRevokeStatement(stmt, user)
			case *influxql.CreateRetentionPolicyStatement:
				res = s.executeCreateRetentionPolicyStatement(stmt, user)
			case *influxql.AlterRetentionPolicyStatement:
				res = s.executeAlterRetentionPolicyStatement(stmt, user)
			case *influxql.DropRetentionPolicyStatement:
				res = s.executeDropRetentionPolicyStatement(stmt, user)
			case *influxql.ShowRetentionPoliciesStatement:
				res = s.executeShowRetentionPoliciesStatement(stmt, user)
			case *influxql.CreateContinuousQueryStatement:
				res = s.executeCreateContinuousQueryStatement(stmt, user)
			case *influxql.DropContinuousQueryStatement:
				continue
			case *influxql.ShowContinuousQueriesStatement:
				res = s.executeShowContinuousQueriesStatement(stmt, database, user)
			default:
				panic(fmt.Sprintf("unsupported statement type: %T", stmt))
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
		}

		// if there was an error send results that the remaining statements weren't executed
		for ; i < len(q.Statements)-1; i++ {
			results <- &Result{Err: ErrNotExecuted}
		}

		s.stats.Inc("queriesExecuted")
		close(results)
	}()

	return results, nil
}

// executeSelectStatement plans and executes a select statement against a database.
func (s *Server) executeSelectStatement(statementID int, stmt *influxql.SelectStatement, database string, user *User, results chan *Result, chunkSize int) error {
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
			results <- &Result{StatementID: statementID, Series: []*influxql.Row{row}}
		}
	}

	if !resultSent {
		results <- &Result{StatementID: statementID, Series: make([]*influxql.Row, 0)}
	}

	return nil
}

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

	return stmt, nil
}

// expandWildcards returns a new SelectStatement with wildcards in the fields
// and/or GROUP BY exapnded with actual field names.
func (s *Server) expandWildcards(stmt *influxql.SelectStatement) (*influxql.SelectStatement, error) {
	// If there are no wildcards in the statement, return it as-is.
	if !stmt.HasWildcard() {
		return stmt, nil
	}

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
func (s *Server) planSelectStatement(stmt *influxql.SelectStatement, chunkSize int) (*influxql.Executor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Plan query.
	p := influxql.NewPlanner(s)

	return p.Plan(stmt, chunkSize)
}

func (s *Server) executeCreateDatabaseStatement(q *influxql.CreateDatabaseStatement, user *User) *Result {
	return &Result{Err: s.CreateDatabase(q.Name)}
}

func (s *Server) executeDropDatabaseStatement(q *influxql.DropDatabaseStatement, user *User) *Result {
	return &Result{Err: s.DropDatabase(q.Name)}
}

func (s *Server) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, user *User) *Result {
	row := &influxql.Row{Name: "databases", Columns: []string{"name"}}
	for _, name := range s.Databases() {
		row.Values = append(row.Values, []interface{}{name})
	}
	return &Result{Series: []*influxql.Row{row}}
}

func (s *Server) executeShowServersStatement(q *influxql.ShowServersStatement, user *User) *Result {
	row := &influxql.Row{Columns: []string{"id", "url"}}
	for _, node := range s.DataNodes() {
		row.Values = append(row.Values, []interface{}{node.ID, node.URL.String()})
	}
	return &Result{Series: []*influxql.Row{row}}
}

func (s *Server) executeCreateUserStatement(q *influxql.CreateUserStatement, user *User) *Result {
	isAdmin := false
	if q.Privilege != nil {
		isAdmin = *q.Privilege == influxql.AllPrivileges
	}
	return &Result{Err: s.CreateUser(q.Name, q.Password, isAdmin)}
}

func (s *Server) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement, user *User) *Result {
	return &Result{Err: s.UpdateUser(q.Name, q.Password)}
}

func (s *Server) executeDropUserStatement(q *influxql.DropUserStatement, user *User) *Result {
	return &Result{Err: s.DeleteUser(q.Name)}
}

func (s *Server) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string, user *User) *Result {
	return &Result{Err: s.DropMeasurement(database, stmt.Name)}
}

func (s *Server) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string, user *User) *Result {
	s.mu.RLock()

	seriesByMeasurement := make(map[string][]uint64)
	// Handle the simple `DROP SERIES <id>` case.
	if stmt.Source == nil && stmt.Condition == nil {
		for _, db := range s.databases {
			for _, m := range db.measurements {
				if m.seriesByID[stmt.SeriesID] != nil {
					seriesByMeasurement[m.Name] = []uint64{stmt.SeriesID}
				}
			}
		}

		s.mu.RUnlock()
		return &Result{Err: s.DropSeries(database, seriesByMeasurement)}
	}

	// Handle the more complicated `DROP SERIES` with sources and/or conditions...

	// Find the database.
	db := s.databases[database]
	if db == nil {
		s.mu.RUnlock()
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		s.mu.RUnlock()
		return &Result{Err: err}
	}

	for _, m := range measurements {
		var ids seriesIDs
		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			filters := map[uint64]influxql.Expr{}
			ids, _, _ = m.walkWhereForSeriesIds(stmt.Condition, filters)

			// TODO: check return of walkWhereForSeriesIds for fields
		} else {
			// No WHERE clause so get all series IDs for this measurement.
			ids = m.seriesIDs
		}

		seriesByMeasurement[m.Name] = ids
	}
	s.mu.RUnlock()

	return &Result{Err: s.DropSeries(database, seriesByMeasurement)}
}

func (s *Server) executeShowSeriesStatement(stmt *influxql.ShowSeriesStatement, database string, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the database.
	db := s.databases[database]
	if db == nil {
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &Result{Err: err}
	}

	// Create result struct that will be populated and returned.
	result := &Result{
		Series: make(influxql.Rows, 0, len(measurements)),
	}

	// Loop through measurements to build result. One result row / measurement.
	for _, m := range measurements {
		var ids seriesIDs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			filters := map[uint64]influxql.Expr{}
			ids, _, _ = m.walkWhereForSeriesIds(stmt.Condition, filters)

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

func (s *Server) executeShowMeasurementsStatement(stmt *influxql.ShowMeasurementsStatement, database string, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the database.
	db := s.databases[database]
	if db == nil {
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	var measurements Measurements

	// If a WHERE clause was specified, filter the measurements.
	if stmt.Condition != nil {
		var err error
		measurements, err = db.measurementsByExpr(stmt.Condition)
		if err != nil {
			return &Result{Err: err}
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
		return &Result{}
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
	result := &Result{
		Series: influxql.Rows{row},
	}

	return result
}

func (s *Server) executeShowTagKeysStatement(stmt *influxql.ShowTagKeysStatement, database string, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the database.
	db := s.databases[database]
	if db == nil {
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &Result{Err: err}
	}

	// Make result.
	result := &Result{
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
}

func (s *Server) executeShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the database.
	db := s.databases[database]
	if db == nil {
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &Result{Err: err}
	}

	// Make result.
	result := &Result{
		Series: make(influxql.Rows, 0),
	}

	tagValues := make(map[string]stringSet)
	for _, m := range measurements {
		var ids seriesIDs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			filters := map[uint64]influxql.Expr{}
			ids, _, _ = m.walkWhereForSeriesIds(stmt.Condition, filters)

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
}

func (s *Server) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement, database string, user *User) *Result {
	rows := []*influxql.Row{}
	for _, name := range s.Databases() {
		row := &influxql.Row{Columns: []string{"name", "query"}, Name: name}
		for _, cq := range s.ContinuousQueries(name) {
			row.Values = append(row.Values, []interface{}{cq.cq.Name, cq.Query})
		}
		rows = append(rows, row)
	}
	return &Result{Series: rows}
}

func (s *Server) executeShowStatsStatement(stmt *influxql.ShowStatsStatement, user *User) *Result {
	var rows []*influxql.Row
	// Server stats.
	serverRow := &influxql.Row{Columns: []string{}}
	serverRow.Name = s.stats.Name()
	s.stats.Walk(func(k string, v int64) {
		serverRow.Columns = append(serverRow.Columns, k)
		serverRow.Values = append(serverRow.Values, []interface{}{v})
	})
	rows = append(rows, serverRow)

	// Shard-level stats.
	for _, sh := range s.shards {
		row := &influxql.Row{Columns: []string{}}
		row.Name = sh.stats.Name()
		sh.stats.Walk(func(k string, v int64) {
			row.Columns = append(row.Columns, k)
			row.Values = append(row.Values, []interface{}{v})
		})
		rows = append(rows, row)
	}

	return &Result{Series: rows}
}

func (s *Server) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &Result{Series: s.DiagnosticsAsRows()}
}

// filterMeasurementsByExpr filters a list of measurements by a tags expression.
func filterMeasurementsByExpr(measurements Measurements, expr influxql.Expr) (Measurements, error) {
	// Create a list to hold result measurements.
	filtered := Measurements{}
	// Iterate measurements adding the ones that match to the result.
	for _, m := range measurements {
		// Look up series IDs that match the tags expression.
		ids, err := m.seriesIDsAllOrByExpr(expr)
		if err != nil {
			return nil, err
		} else if len(ids) > 0 {
			filtered = append(filtered, m)
		}
	}
	sort.Sort(filtered)

	return filtered, nil
}

func (s *Server) executeShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string, user *User) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var err error

	// Find the database.
	db := s.databases[database]
	if db == nil {
		return &Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &Result{Err: err}
	}

	// Make result.
	result := &Result{
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
}

func (s *Server) executeGrantStatement(stmt *influxql.GrantStatement, user *User) *Result {
	return &Result{Err: s.SetPrivilege(stmt.Privilege, stmt.User, stmt.On)}
}

func (s *Server) executeRevokeStatement(stmt *influxql.RevokeStatement, user *User) *Result {
	return &Result{Err: s.SetPrivilege(influxql.NoPrivileges, stmt.User, stmt.On)}
}

// measurementsFromSourceOrDB returns a list of measurements from the
// statement passed in or, if the statement is nil, a list of all
// measurement names from the database passed in.
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

func (s *Server) executeShowUsersStatement(q *influxql.ShowUsersStatement, user *User) *Result {
	row := &influxql.Row{Columns: []string{"user", "admin"}}
	for _, user := range s.Users() {
		row.Values = append(row.Values, []interface{}{user.Name, user.Admin})
	}
	return &Result{Series: []*influxql.Row{row}}
}

func (s *Server) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement, user *User) *Result {
	rp := NewRetentionPolicy(stmt.Name)
	rp.Duration = stmt.Duration
	rp.ReplicaN = uint32(stmt.Replication)

	// Create new retention policy.
	err := s.CreateRetentionPolicy(stmt.Database, rp)
	if err != nil {
		return &Result{Err: err}
	}

	// If requested, set new policy as the default.
	if stmt.Default {
		err = s.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &Result{Err: err}
}

func (s *Server) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement, user *User) *Result {
	rpu := &RetentionPolicyUpdate{
		Duration: stmt.Duration,
		ReplicaN: func() *uint32 {
			if stmt.Replication == nil {
				return nil
			}
			n := uint32(*stmt.Replication)
			return &n
		}(),
	}

	// Update the retention policy.
	err := s.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu)
	if err != nil {
		return &Result{Err: err}
	}

	// If requested, set as default retention policy.
	if stmt.Default {
		err = s.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &Result{Err: err}
}

func (s *Server) executeDeleteStatement() *Result {
	return &Result{Err: ErrInvalidQuery}
}

func (s *Server) executeDropRetentionPolicyStatement(q *influxql.DropRetentionPolicyStatement, user *User) *Result {
	return &Result{Err: s.DeleteRetentionPolicy(q.Database, q.Name)}
}

func (s *Server) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement, user *User) *Result {
	a, err := s.RetentionPolicies(q.Database)
	if err != nil {
		return &Result{Err: err}
	}

	d := s.databases[q.Database]

	row := &influxql.Row{Columns: []string{"name", "duration", "replicaN", "default"}}
	for _, rp := range a {
		row.Values = append(row.Values, []interface{}{rp.Name, rp.Duration.String(), rp.ReplicaN, d.defaultRetentionPolicy == rp.Name})
	}
	return &Result{Series: []*influxql.Row{row}}
}

func (s *Server) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement, user *User) *Result {
	return &Result{Err: s.CreateContinuousQuery(q)}
}

// CreateContinuousQuery creates a continuous query.
func (s *Server) CreateContinuousQuery(q *influxql.CreateContinuousQueryStatement) error {
	c := &createContinuousQueryCommand{Query: q.String()}
	_, err := s.broadcast(createContinuousQueryMessageType, c)
	return err
}

func (s *Server) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement, user *User) *Result {
	return &Result{Err: s.DropContinuousQuery(q)}
}

// DropContinuousQuery dropsoa continuous query.
func (s *Server) DropContinuousQuery(q *influxql.DropContinuousQueryStatement) error {
	c := &dropContinuousQueryCommand{Name: q.Name, Database: q.Database}
	_, err := s.broadcast(dropContinuousQueryMessageType, c)
	return err
}

// ContinuousQueries returns a list of all continuous queries.
func (s *Server) ContinuousQueries(database string) []*ContinuousQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()

	db := s.databases[database]
	if db == nil {
		return nil
	}

	return db.continuousQueries
}

// MeasurementNames returns a list of all measurements for the specified database.
func (s *Server) MeasurementNames(database string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	db := s.databases[database]
	if db == nil {
		return nil
	}

	return db.names
}

// measurement returns a measurement by database and name.
func (s *Server) measurement(database, name string) (*Measurement, error) {
	db := s.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound(database)
	}

	return db.measurements[name], nil
}

// Begin returns an unopened transaction associated with the server.
func (s *Server) Begin() (influxql.Tx, error) { return newTx(s), nil }

// StartLocalMapper will create a local mapper for the passed in remote mapper
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
		tmax:         rm.TMax,
		limit:        limit,
	}

	return lm, nil
}

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
	db := s.databases[m.Database]
	if db == nil {
		return ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if db.defaultRetentionPolicy == "" {
			return fmt.Errorf("default retention policy not set for: %s", db.name)
		}
		m.RetentionPolicy = db.defaultRetentionPolicy
	}

	// Make sure the retention policy exists.
	if _, ok := db.policies[m.RetentionPolicy]; !ok {
		return fmt.Errorf("retention policy does not exist: %s.%s", m.Database, m.RetentionPolicy)
	}

	return nil
}

// DiagnosticsAsRows returns diagnostic information about the server, as a slice of
// InfluxQL rows.
func (s *Server) DiagnosticsAsRows() []*influxql.Row {
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
			shardsRow.Values = append(shardsRow.Values, []interface{}{now, strconv.FormatUint(sh.ID, 10), strings.Join(nodes, ","),
				strconv.FormatUint(sh.index, 10), sh.store.Path()})
		}
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
}

// processor runs in a separate goroutine and processes all incoming broker messages.
func (s *Server) processor(conn MessagingConn, done chan struct{}) {
	for {
		// Read incoming message.
		var m *messaging.Message
		var ok bool
		select {
		case <-done:
			return
		case m, ok = <-conn.C():
			if !ok {
				return
			}
		}

		// All messages must be processed under lock.
		func() {
			s.stats.Inc("broadcastMessageRx")
			s.mu.Lock()
			defer s.mu.Unlock()

			// Exit if closed or if the index is below the high water mark.
			if !s.opened() {
				return
			} else if s.index >= m.Index {
				return
			}

			// Process message.
			var err error
			switch m.Type {
			case createDataNodeMessageType:
				err = s.applyCreateDataNode(m)
			case deleteDataNodeMessageType:
				err = s.applyDeleteDataNode(m)
			case createDatabaseMessageType:
				err = s.applyCreateDatabase(m)
			case dropDatabaseMessageType:
				err = s.applyDropDatabase(m)
			case createUserMessageType:
				err = s.applyCreateUser(m)
			case updateUserMessageType:
				err = s.applyUpdateUser(m)
			case deleteUserMessageType:
				err = s.applyDeleteUser(m)
			case createRetentionPolicyMessageType:
				err = s.applyCreateRetentionPolicy(m)
			case updateRetentionPolicyMessageType:
				err = s.applyUpdateRetentionPolicy(m)
			case deleteRetentionPolicyMessageType:
				err = s.applyDeleteRetentionPolicy(m)
			case createShardGroupIfNotExistsMessageType:
				err = s.applyCreateShardGroupIfNotExists(m)
			case deleteShardGroupMessageType:
				err = s.applyDeleteShardGroup(m)
			case setDefaultRetentionPolicyMessageType:
				err = s.applySetDefaultRetentionPolicy(m)
			case createMeasurementsIfNotExistsMessageType:
				err = s.applyCreateMeasurementsIfNotExists(m)
			case dropMeasurementMessageType:
				err = s.applyDropMeasurement(m)
			case setPrivilegeMessageType:
				err = s.applySetPrivilege(m)
			case createContinuousQueryMessageType:
				err = s.applyCreateContinuousQueryCommand(m)
			case dropContinuousQueryMessageType:
				err = s.applyDropContinuousQueryCommand(m)
			case dropSeriesMessageType:
				err = s.applyDropSeries(m)
			case writeRawSeriesMessageType:
				panic("write series not allowed in broadcast topic")
			}

			// Sync high water mark and errors.
			s.index = m.Index
			if err != nil {
				s.errors[m.Index] = err
			}
		}()
	}
}

// Result represents a resultset returned from a single statement.
type Result struct {
	// StatementID is just the statement's position in the query. It's used
	// to combine statement results if they're being buffered in memory.
	StatementID int `json:"-"`
	Series      influxql.Rows
	Err         error
}

// MarshalJSON encodes the result into JSON.
func (r *Result) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Series []*influxql.Row `json:"series,omitempty"`
		Err    string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Series = r.Series
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Result struct
func (r *Result) UnmarshalJSON(b []byte) error {
	var o struct {
		Series []*influxql.Row `json:"series,omitempty"`
		Err    string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Series = o.Series
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Response represents a list of statement results.
type Response struct {
	Results []*Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*Result `json:"results,omitempty"`
		Err     string    `json:"error,omitempty"`
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
		Results []*Result `json:"results,omitempty"`
		Err     string    `json:"error,omitempty"`
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

// MessagingClient represents the client used to connect to brokers.
type MessagingClient interface {
	Open(path string) error
	Close() error

	// Retrieves or sets the current list of broker URLs.
	URLs() []url.URL
	SetURLs([]url.URL)

	// Publishes a message to the broker.
	Publish(m *messaging.Message) (index uint64, err error)

	// Conn returns an open, streaming connection to a topic.
	Conn(topicID uint64) MessagingConn
}

type messagingClient struct {
	*messaging.Client
}

// NewMessagingClient returns an instance of MessagingClient.
func NewMessagingClient(dataURL url.URL) MessagingClient {
	return &messagingClient{messaging.NewClient(dataURL)}
}

func (c *messagingClient) Conn(topicID uint64) MessagingConn { return c.Client.Conn(topicID) }

// MessagingConn represents a streaming connection to a single broker topic.
type MessagingConn interface {
	Open(index uint64, streaming bool) error
	C() <-chan *messaging.Message
}

// DataNode represents a data node in the cluster.
type DataNode struct {
	ID  uint64
	URL *url.URL
}

// newDataNode returns an instance of DataNode.
func newDataNode() *DataNode { return &DataNode{} }

type dataNodes []*DataNode

func (p dataNodes) Len() int           { return len(p) }
func (p dataNodes) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p dataNodes) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Authorize user u to execute query q on database.
// database can be "" for queries that do not require a database.
// If u is nil, this means authorization is disabled.
func (s *Server) Authorize(u *User, q *influxql.Query, database string) error {
	const authErrLogFmt = `unauthorized request | user: %q | query: %q | database %q\n`

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

// BcryptCost is the cost associated with generating password with Bcrypt.
// This setting is lowered during testing to improve test suite performance.
var BcryptCost = 10

// User represents a user account on the system.
// It can be given read/write permissions to individual databases.
type User struct {
	Name       string                        `json:"name"`
	Hash       string                        `json:"hash"`
	Privileges map[string]influxql.Privilege `json:"privileges"` // db name to privilege
	Admin      bool                          `json:"admin,omitempty"`
}

// Authenticate returns nil if the password matches the user's password.
// Returns an error if the password was incorrect.
func (u *User) Authenticate(password string) error {
	return bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password))
}

// Authorize returns true if the user is authorized and false if not.
func (u *User) Authorize(privilege influxql.Privilege, database string) bool {
	p, ok := u.Privileges[database]
	return (ok && p >= privilege) || (u.Admin)
}

// users represents a list of users, sortable by name.
type users []*User

func (p users) Len() int           { return len(p) }
func (p users) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p users) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

// HashPassword generates a cryptographically secure hash for password.
// Returns an error if the password is invalid or a hash cannot be generated.
func HashPassword(password string) ([]byte, error) {
	// The second arg is the cost of the hashing, higher is slower but makes
	// it harder to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
}

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
		return nil, errors.New("query isn't a valie continuous query")
	}

	cquery := &ContinuousQuery{
		Query: q,
		cq:    cq,
	}

	return cquery, nil
}

// applyCreateContinuousQueryCommand adds the continuous query to the database object and saves it to the metastore
func (s *Server) applyCreateContinuousQueryCommand(m *messaging.Message) error {
	var c createContinuousQueryCommand
	mustUnmarshalJSON(m.Data, &c)

	cq, err := NewContinuousQuery(c.Query)
	if err != nil {
		return err
	}

	// normalize the select statement in the CQ so that it has the database and retention policy inserted
	if err := s.normalizeStatement(cq.cq.Source, cq.cq.Database); err != nil {
		return err
	}

	// ensure the into database exists
	if s.databases[cq.intoDB()] == nil {
		return ErrDatabaseNotFound(cq.intoDB())
	}

	// Retrieve the database.
	db := s.databases[cq.cq.Database]
	if db == nil {
		return ErrDatabaseNotFound(cq.cq.Database)
	} else if db.continuousQueryByName(cq.cq.Name) != nil {
		return ErrContinuousQueryExists
	}

	// Add cq to the database.
	db.continuousQueries = append(db.continuousQueries, cq)

	// Persist to metastore.
	s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

// applyDropContinuousQueryCommand removes the continuous query from the database object and saves it to the metastore
func (s *Server) applyDropContinuousQueryCommand(m *messaging.Message) error {
	var c dropContinuousQueryCommand

	mustUnmarshalJSON(m.Data, &c)

	// retrieve the database and ensure that it exists
	db := s.databases[c.Database]
	if db == nil {
		return ErrDatabaseNotFound(c.Database)
	}

	// loop through continuous queries and find the match
	cqIndex := -1
	for n, continuousQuery := range db.continuousQueries {
		if continuousQuery.cq.Name == c.Name {
			cqIndex = n
			break
		}
	}

	if cqIndex == -1 {
		return ErrContinuousQueryNotFound
	}

	// delete the relevant continuous query
	copy(db.continuousQueries[cqIndex:], db.continuousQueries[cqIndex+1:])
	db.continuousQueries[len(db.continuousQueries)-1] = nil
	db.continuousQueries = db.continuousQueries[:len(db.continuousQueries)-1]

	// persist to metastore
	s.meta.mustUpdate(m.Index, func(tx *metatx) error {
		return tx.saveDatabase(db)
	})

	return nil
}

// RunContinuousQueries will run any continuous queries that are due to run and write the
// results back into the database
func (s *Server) RunContinuousQueries() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, d := range s.databases {
		for _, c := range d.continuousQueries {
			if s.shouldRunContinuousQuery(c) {
				// set the into retention policy based on what is now the default
				if c.intoRP() == "" {
					c.setIntoRP(d.defaultRetentionPolicy)
				}
				go func(cq *ContinuousQuery) {
					s.runContinuousQuery(c)
				}(c)
			}
		}
	}

	return nil
}

// shouldRunContinuousQuery returns true if the CQ should be schedule to run. It will use the
// lastRunTime of the CQ and the rules for when to run set through the config to determine
// if this CQ should be run
func (s *Server) shouldRunContinuousQuery(cq *ContinuousQuery) bool {
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
	computeEvery := time.Duration(interval.Nanoseconds()/int64(s.ComputeRunsPerInterval)) * time.Nanosecond
	// make sure we're running no more frequently than the setting in the config
	if computeEvery < s.ComputeNoMoreThan {
		computeEvery = s.ComputeNoMoreThan
	}

	// if we've passed the amount of time since the last run, do it up
	if cq.lastRun.Add(computeEvery).UnixNano() <= time.Now().UnixNano() {
		return true
	}

	return false
}

// runContinuousQuery will execute a continuous query
// TODO: make this fan out to the cluster instead of running all the queries on this single data node
func (s *Server) runContinuousQuery(cq *ContinuousQuery) {
	s.stats.Inc("continuousQueryExecuted")
	cq.mu.Lock()
	defer cq.mu.Unlock()

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

// runContinuousQueryAndWriteResult will run the query against the cluster and write the results back in
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
				for _, v := range p.Fields {
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

// convertRowToPoints will convert a query result Row into Points that can be written back in.
// Used for continuous and INTO queries
func (s *Server) convertRowToPoints(measurementName string, row *influxql.Row) ([]Point, error) {
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

	points := make([]Point, 0, len(row.Values))
	for _, v := range row.Values {
		vals := make(map[string]interface{})
		for fieldName, fieldIndex := range fieldIndexes {
			vals[fieldName] = v[fieldIndex]
		}

		p := &Point{
			Name:      measurementName,
			Tags:      row.Tags,
			Timestamp: v[timeIndex].(time.Time),
			Fields:    vals,
		}

		points = append(points, *p)
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
}

// CreateSnapshotWriter returns a writer for the current snapshot.
func (s *Server) CreateSnapshotWriter() (*SnapshotWriter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return createServerSnapshotWriter(s)
}

func (s *Server) URL() url.URL {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.dataNodes[s.id]; n != nil {
		return *n.URL
	}
	return url.URL{}
}
