// Package snapshotter provides the meta snapshot service.
package snapshotter // import "github.com/influxdata/influxdb/services/snapshotter"

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
	"io"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader = 3

	// BackupMagicHeader is the first 8 bytes used to identify and validate
	// a metastore backup file
	BackupMagicHeader = 0x59590101
)

// Service manages the listener for the snapshot endpoint.
type Service struct {
	wg  sync.WaitGroup
	err chan error

	Node *influxdb.Node

	MetaClient interface {
		encoding.BinaryMarshaler
		Database(name string) *meta.DatabaseInfo
	}

	TSDBStore *tsdb.Store

	Listener net.Listener
	Logger   zap.Logger
}

// NewService returns a new instance of Service.
func NewService() *Service {
	return &Service{
		err:    make(chan error),
		Logger: zap.New(zap.NullEncoder()),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting snapshot service")

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "snapshot"))
}

// Err returns a channel for fatal out-of-band errors.
func (s *Service) Err() <-chan error { return s.err }

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()

		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("snapshot listener closed")
			return
		} else if err != nil {
			s.Logger.Info(fmt.Sprint("error accepting snapshot request: ", err.Error()))
			continue
		}

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info(err.Error())
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var Type [1]byte

	_, err := conn.Read(Type[:])

	if err != nil {
		return err
	}

	switch RequestType(Type[0]) {
	case RequestShardUpdate:
		return s.updateShardsLive(conn)
	}

	r, bytes, err := s.readRequest(conn)
	if err != nil {
		return fmt.Errorf("read request: %s", err)
	}

	switch RequestType(Type[0]) {
	case RequestShardBackup:
		if err := s.TSDBStore.BackupShard(r.ShardID, r.Since, conn); err != nil {
			return err
		}
	case RequestShardExport:
		if err := s.TSDBStore.ExportShard(r.ShardID, r.ExportStart, r.ExportEnd, conn); err != nil {
			return err
		}
	case RequestMetastoreBackup:
		if err := s.writeMetaStore(conn, r.Database); err != nil {
			return err
		}
	case RequestDatabaseInfo:
		return s.writeDatabaseInfo(conn, r.Database)
	case RequestRetentionPolicyInfo:
		return s.writeRetentionPolicyInfo(conn, r.Database, r.RetentionPolicy)
	case RequestMetaStoreUpdate:
		return s.updateMetaStore(conn, bytes, r.Database, r.RetentionPolicy)
	default:
		return fmt.Errorf("request type unknown: %v", r.Type)
	}

	return nil
}

func (s *Service) updateShardsLive(conn net.Conn) error {
	var sidBytes [8]byte
	_, err := conn.Read(sidBytes[:])
	if err != nil {
		return err
	}
	sid := binary.BigEndian.Uint64(sidBytes[:])
	if err := s.TSDBStore.RestoreShard(sid, conn); err != nil {
		return err
	}
	if err := s.TSDBStore.SetShardEnabled(sid, true); err != nil {
		return err
	}
	return nil
}

func (s *Service) updateMetaStore(conn net.Conn, bits []byte, newDBName, newRPName string) error {
	md := meta.Data{}
	err := md.UnmarshalBinary(bits)
	if err != nil {
		s.respondIDMap(conn, map[uint64]uint64{})
		return fmt.Errorf("failed to decode meta: %s", err)
	}

	data := s.MetaClient.(*meta.Client).Data()

	IDMap, err := data.ImportData(md, newDBName, newRPName)
	if err != nil {
		s.respondIDMap(conn, map[uint64]uint64{})
		return err
	}

	rpName := data.Database(newDBName).DefaultRetentionPolicy
	err = s.MetaClient.(*meta.Client).SetData(&data)

	for _, v := range IDMap {
		s.TSDBStore.CreateShard(newDBName, rpName, v, true)
	}

	err = s.respondIDMap(conn, IDMap)
	return err
}

// send the IDMapping based on the metadata from the source server vs the shard ID
// metadata on this server.  Sends back [BackupMagicHeader,0] if there's no mapped
// values, signaling that nothing should be imported.
func (s *Service) respondIDMap(conn net.Conn, IDMap map[uint64]uint64) error {
	npairs := len(IDMap)
	// 2 information ints, then npairs of 8byte ints.
	numBytes := make([]byte, (npairs+1)*16)

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(npairs))
	next := 16
	for k, v := range IDMap {
		binary.BigEndian.PutUint64(numBytes[next:next+8], k)
		binary.BigEndian.PutUint64(numBytes[next+8:next+16], v)
		next += 16
	}

	if _, err := conn.Write(numBytes[:]); err != nil {
		return err
	}
	return nil
}

func (s *Service) writeMetaStore(conn net.Conn, dbName string) error {
	// Retrieve and serialize the current meta data.
	// if the dbName is non-empty, then we drop all the DB's from the metadata
	// that aren't being exported.

	x := s.MetaClient.(*meta.Client)
	data := x.Data()

	if dbName != "" {
		keepDB := data.Database(dbName)
		if keepDB == nil {
			return fmt.Errorf("database %s not found", dbName)
		}
		data.Databases = []meta.DatabaseInfo{*keepDB}
		// drop all users, since we only want the DB
		data.Users = []meta.UserInfo{}

	}
	metaBlob, err := data.MarshalBinary()

	if err != nil {
		return fmt.Errorf("marshal meta: %s", err)
	}

	var nodeBytes bytes.Buffer
	if err := json.NewEncoder(&nodeBytes).Encode(s.Node); err != nil {
		return err
	}

	var numBytes [24]byte

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(len(metaBlob)))
	binary.BigEndian.PutUint64(numBytes[16:24], uint64(nodeBytes.Len()))

	// backup header followed by meta blob length
	if _, err := conn.Write(numBytes[:16]); err != nil {
		return err
	}

	if _, err := conn.Write(metaBlob); err != nil {
		return err
	}

	if _, err := conn.Write(numBytes[16:24]); err != nil {
		return err
	}

	if _, err := nodeBytes.WriteTo(conn); err != nil {
		return err
	}
	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the database on
// this server into the connection.
func (s *Service) writeDatabaseInfo(conn net.Conn, database string) error {
	res := Response{}
	db := s.MetaClient.Database(database)
	if db == nil {
		return influxdb.ErrDatabaseNotFound(database)
	}

	for _, rp := range db.RetentionPolicies {
		for _, sg := range rp.ShardGroups {
			for _, sh := range sg.Shards {
				// ignore if the shard isn't on the server
				if s.TSDBStore.Shard(sh.ID) == nil {
					continue
				}

				path, err := s.TSDBStore.ShardRelativePath(sh.ID)
				if err != nil {
					return err
				}

				res.Paths = append(res.Paths, path)
			}
		}
	}

	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the retention policy on
// this server into the connection
func (s *Service) writeRetentionPolicyInfo(conn net.Conn, database, retentionPolicy string) error {
	res := Response{}
	db := s.MetaClient.Database(database)
	if db == nil {
		return influxdb.ErrDatabaseNotFound(database)
	}

	var ret *meta.RetentionPolicyInfo

	for _, rp := range db.RetentionPolicies {
		if rp.Name == retentionPolicy {
			ret = &rp
			break
		}
	}

	if ret == nil {
		return influxdb.ErrRetentionPolicyNotFound(retentionPolicy)
	}

	for _, sg := range ret.ShardGroups {
		for _, sh := range sg.Shards {
			// ignore if the shard isn't on the server
			if s.TSDBStore.Shard(sh.ID) == nil {
				continue
			}

			path, err := s.TSDBStore.ShardRelativePath(sh.ID)
			if err != nil {
				return err
			}

			res.Paths = append(res.Paths, path)
		}
	}

	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

// readRequest unmarshals a request object from the conn.
func (s *Service) readRequest(conn net.Conn) (Request, []byte, error) {
	var r Request
	d := json.NewDecoder(conn)

	if err := d.Decode(&r); err != nil {
		return r, []byte{}, err
	}

	bits := make([]byte, r.UploadSize+1)

	if r.UploadSize > 0 {

		remainder := d.Buffered()

		//// no time to investigate.  It seems the JSON encoder puts one extra byte on the stream,
		//// but the decoder does not consume it.  so we eat a byte here and then everything works.
		//onebyte := bufio.NewReader(remainder)
		//_, err := onebyte.ReadByte()
		//if err != nil {
		//	return r, bits, err
		//}

		n, err := remainder.Read(bits)
		if err != nil && err != io.EOF {
			return r, bits, err
		}

		// it is a bit random but sometimes the Json decoder will consume all the bytes and sometimes
		// it will leave a few behind.
		if err != io.EOF && n < int(r.UploadSize+1) {
			n, err = conn.Read(bits[n:])
		}

		if err != nil && err != io.EOF {
			return r, bits, err
		}
		return r, bits[1:], nil
	}

	return r, bits, nil
}

// RequestType indicates the typeof snapshot request.
type RequestType uint8

const (
	// RequestShardBackup represents a request for a shard backup.
	RequestShardBackup RequestType = iota

	// RequestMetastoreBackup represents a request to back up the metastore.
	RequestMetastoreBackup

	// RequestDatabaseInfo represents a request for database info.
	RequestDatabaseInfo

	// RequestRetentionPolicyInfo represents a request for retention policy info.
	RequestRetentionPolicyInfo

	// RequestShardExport represents a request to export Shard data.  Similar to a backup, but shards
	// may be filtered based on the start/end times on each block.
	RequestShardExport

	// RequestMetaStoreUpdate represents a request to upload a metafile that will be used to do a live update
	// to the existing metastore.
	RequestMetaStoreUpdate

	// RequestShardUpdate will initiate the upload of a shard data tar file
	// and have the engine import the data.
	RequestShardUpdate
)

// Request represents a request for a specific backup or for information
// about the shards on this server for a database or retention policy.
type Request struct {
	Type            RequestType
	Database        string
	RetentionPolicy string
	ShardID         uint64
	Since           time.Time
	ExportStart     time.Time
	ExportEnd       time.Time
	UploadSize      int64
}

// Response contains the relative paths for all the shards on this server
// that are in the requested database or retention policy.
type Response struct {
	Paths []string
}
