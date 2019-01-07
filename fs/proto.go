package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/influxdata/platform"
	"go.uber.org/zap"
)

const protoFileExt = ".json"

// ProtoService implements platform.ProtoService on the file system.
type ProtoService struct {
	Dir              string
	DashboardService platform.DashboardService
	Logger           *zap.Logger

	protos []*platform.Proto
}

// NewProtoService creates an instance of a ProtoService.
func NewProtoService(dir string, logger *zap.Logger, s platform.DashboardService) *ProtoService {
	return &ProtoService{
		Dir:              dir,
		Logger:           logger.With(zap.String("service", "proto")),
		DashboardService: s,
	}
}

// WithProtos is used for testing the ProtoService. It will overwrite the protos on the service.
func (s *ProtoService) WithProtos(ps []*platform.Proto) {
	s.protos = ps
}

// Open loads the protos from the file system and sets them on the service.
func (s *ProtoService) Open(ctx context.Context) error {
	if _, err := os.Stat(s.Dir); os.IsNotExist(err) {
		if err := os.Mkdir(s.Dir, 0600); err != nil {
			return err
		}
	}

	files, err := ioutil.ReadDir(s.Dir)
	if err != nil {
		return err
	}

	protos := []*platform.Proto{}
	for _, file := range files {
		filename := file.Name()
		if path.Ext(filename) != protoFileExt {
			s.Logger.Info("file extention did not match proto file extension", zap.String("file", filename))
			continue
		}

		octets, err := ioutil.ReadFile(filepath.Join(s.Dir, filename))
		if err != nil {
			s.Logger.Info("error openeing file", zap.String("file", filename), zap.Error(err))
			continue
		}

		proto := &platform.Proto{}
		if err := json.Unmarshal(octets, proto); err != nil {
			s.Logger.Info("error unmarshalling file into proto", zap.String("file", filename), zap.Error(err))
			continue
		}

		// TODO(desa): ensure that the proto provided is a valid proto (e.g. that all viewID exists for all cells in a dashboard).

		protos = append(protos, proto)
	}

	s.protos = protos

	return nil
}

// FindProtos returns a list of protos from the file system.
func (s *ProtoService) FindProtos(ctx context.Context) ([]*platform.Proto, error) {
	protos := []*platform.Proto{}
	for _, proto := range s.protos {
		// easy way to make a deep copy
		octets, err := json.Marshal(proto)
		if err != nil {
			return nil, err
		}

		p := &platform.Proto{}
		if err := json.Unmarshal(octets, p); err != nil {
			return nil, err
		}

		protos = append(protos, p)
	}

	return protos, nil
}

func (s *ProtoService) findProto(ctx context.Context, id platform.ID) (*platform.Proto, error) {
	for _, proto := range s.protos {
		if proto.ID == id {
			return proto, nil
		}
	}

	return nil, &platform.Error{Msg: "proto not found"}
}

// CreateDashboardsFromProtos creates instances of each dashboard in a proto.
func (s *ProtoService) CreateDashboardsFromProto(ctx context.Context, protoID platform.ID, orgID platform.ID) ([]*platform.Dashboard, error) {
	// TODO(desa): this should be done transactionally.
	proto, err := s.findProto(ctx, protoID)
	if err != nil {
		return nil, err
	}

	dashes := []*platform.Dashboard{}

	for _, protodash := range proto.Dashboards {
		dash := &platform.Dashboard{}
		*dash = protodash.Dashboard
		// TODO(desa): add organization id here
		dash.Cells = nil

		if err := s.DashboardService.CreateDashboard(ctx, dash); err != nil {
			return nil, err
		}

		cells := []*platform.Cell{}
		for _, protocell := range protodash.Dashboard.Cells {
			cell := &platform.Cell{}
			*cell = *protocell

			protoview, ok := protodash.Views[cell.ID]
			if !ok {
				return nil, &platform.Error{Msg: fmt.Sprintf("view for ID %q does not exist", cell.ID)}
			}

			view := &platform.View{}
			*view = protoview
			opts := platform.AddDashboardCellOptions{View: view}
			if err := s.DashboardService.AddDashboardCell(ctx, dash.ID, cell, opts); err != nil {
				return nil, err
			}

			cells = append(cells, cell)
		}

		dash.Cells = cells

		dashes = append(dashes, dash)
	}

	return dashes, nil
}
