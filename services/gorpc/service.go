package gorpc

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

type InfluxGoRpc struct {
	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}
	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
	}
	Logger *log.Logger

	user        *meta.UserInfo
	database    *meta.DatabaseInfo
	requireAuth bool
}

type Service struct {
	config Config
	ln     net.Listener
	wg     sync.WaitGroup
	InfluxGoRpc
}

func NewService(c Config) *Service {
	return &Service{
		config: c,
		InfluxGoRpc: InfluxGoRpc{
			Logger: log.New(os.Stderr, "[gorpc] ", log.LstdFlags),
		},
	}
}

func (s *Service) Open() (err error) {
	if s.config.Network == "" {
		return errors.New("Network has to be specified in config")
	}
	if s.config.Laddr == "" {
		return errors.New("local address has to be specified in config")
	}

	if s.config.Network == "unix" {
		if info, err := os.Stat(s.config.Laddr); err == nil && (info.Mode()&os.ModeSocket == os.ModeSocket) {
			os.Remove(s.config.Laddr)
		}
	}

	s.ln, err = net.Listen(s.config.Network, s.config.Laddr)
	if err != nil {
		s.Logger.Printf("Failed to set up %s listener at address %s %s", s.config.Network, s.config.Laddr, err)
		return err
	}
	if s.config.Network == "unix" {
		os.Chmod(s.config.Laddr, os.ModeSocket|0777)
	}

	s.Logger.Printf("Started listening on %s:%s", s.config.Network, s.config.Laddr)

	s.wg.Add(1)
	go s.serve()

	return nil
}

func (s *Service) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.ln.Accept()
		if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
			s.Logger.Println("GoRPC listener closed")
			return
		} else if err != nil {
			s.Logger.Printf("Failed to read %s:%s message: %s", s.config.Network, s.config.Laddr, err)
			continue
		}
		i := s.InfluxGoRpc
		i.requireAuth = s.config.AuthEnabled
		sv := rpc.NewServer()
		sv.Register(&i)
		go sv.ServeConn(conn)
	}
}

type PostData []tsdb.GoRpcPoint
type Empty struct{}

func (i *InfluxGoRpc) Post(pd *PostData, reply *Empty) error {
	var err error
	if err = writePrivilege(i); err != nil {
		return err
	}
	points := make([]tsdb.Point, len(*pd))
	for i, point := range *pd {
		if points[i], err = tsdb.NewGoRpcPoint(point); err != nil {
			return err
		}
	}
	err = i.PointsWriter.WritePoints(&cluster.WritePointsRequest{
		Database:         i.database.Name,
		RetentionPolicy:  "",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
		Points:           points,
	})
	if err != nil {
		i.Logger.Println("GoRPC cannot write data: ", err)
	}
	return nil
}

type AuthData struct {
	Database string
	Username string
	Password string
}

func (i *InfluxGoRpc) Auth(ad *AuthData, reply *Empty) (err error) {
	i.user, i.database, err = authenticate(i, ad)
	return
}

func (s *Service) Close() error {
	if s.ln == nil {
		return errors.New("Service already closed")
	}

	s.ln.Close()
	s.ln = nil
	s.wg.Wait()
	s.Logger.Print("Service closed")
	return nil
}

func authenticate(i *InfluxGoRpc, ad *AuthData) (*meta.UserInfo, *meta.DatabaseInfo, error) {
	var (
		user *meta.UserInfo
		db   *meta.DatabaseInfo
		err  error
	)
	if db, err = i.MetaStore.Database(ad.Database); err != nil {
		return user, db, err
	}
	if !i.requireAuth {
		return user, db, nil
	}

	// Retrieve user list.
	uis, err := i.MetaStore.Users()
	if err != nil {
		return user, db, err
	}

	// TODO corylanou: never allow this in the future without users
	if i.requireAuth && len(uis) > 0 {
		if ad.Username == "" {
			return user, db, fmt.Errorf("username required. username:'%s'", ad.Username)
		}
		user, err = i.MetaStore.Authenticate(ad.Username, ad.Password)
		if err != nil {
			return user, db, err
		}
	}
	return user, db, nil
}

func writePrivilege(i *InfluxGoRpc) error {
	if i.requireAuth {
		if i.user == nil {
			return fmt.Errorf("User is not authenticate")
		}
		if i.database == nil {
			return fmt.Errorf("Not found database")
		}
		if !i.user.Authorize(influxql.WritePrivilege, i.database.Name) {
			return fmt.Errorf("%q user is not authorized to write to database %q", i.user.Name, i.database.Name)
		}
	}
	return nil
}
