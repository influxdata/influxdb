package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/cluster/internal"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

//go:generate protoc --gogo_out=. internal/data.proto

// WritePointsRequest represents a request to write point data to the cluster
type WritePointsRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// WriteShardRequest represents the a request to write a slice of points to a shard
type WriteShardRequest struct {
	pb internal.WriteShardRequest
}

// WriteShardResponse represents the response returned from a remote WriteShardRequest call
type WriteShardResponse struct {
	pb internal.WriteShardResponse
}

// SetShardID sets the ShardID
func (w *WriteShardRequest) SetShardID(id uint64) { w.pb.ShardID = &id }

// ShardID gets the ShardID
func (w *WriteShardRequest) ShardID() uint64 { return w.pb.GetShardID() }

func (w *WriteShardRequest) SetDatabase(db string) { w.pb.Database = &db }

func (w *WriteShardRequest) SetRetentionPolicy(rp string) { w.pb.RetentionPolicy = &rp }

func (w *WriteShardRequest) Database() string { return w.pb.GetDatabase() }

func (w *WriteShardRequest) RetentionPolicy() string { return w.pb.GetRetentionPolicy() }

// Points returns the time series Points
func (w *WriteShardRequest) Points() []models.Point { return w.unmarshalPoints() }

// AddPoint adds a new time series point
func (w *WriteShardRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.AddPoints([]models.Point{pt})
}

// AddPoints adds a new time series point
func (w *WriteShardRequest) AddPoints(points []models.Point) {
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			// A error here means that we create a point higher in the stack that we could
			// not marshal to a byte slice.  If that happens, the endpoint that created that
			// point needs to be fixed.
			panic(fmt.Sprintf("failed to marshal point: `%v`: %v", p, err))
		}
		w.pb.Points = append(w.pb.Points, b)
	}
}

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

func (w *WriteShardRequest) unmarshalPoints() []models.Point {
	points := make([]models.Point, len(w.pb.GetPoints()))
	for i, p := range w.pb.GetPoints() {
		pt, err := models.NewPointFromBytes(p)
		if err != nil {
			// A error here means that one node created a valid point and sent us an
			// unparseable version.  We could log and drop the point and allow
			// anti-entropy to resolve the discrepancy, but this shouldn't ever happen.
			panic(fmt.Sprintf("failed to parse point: `%v`: %v", string(p), err))
		}

		points[i] = pt
	}
	return points
}

// SetCode sets the Code
func (w *WriteShardResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// SetMessage sets the Message
func (w *WriteShardResponse) SetMessage(message string) { w.pb.Message = &message }

// Code returns the Code
func (w *WriteShardResponse) Code() int { return int(w.pb.GetCode()) }

// Message returns the Message
func (w *WriteShardResponse) Message() string { return w.pb.GetMessage() }

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementRequest represents the a request to execute a statement on a node.
type ExecuteStatementRequest struct {
	pb internal.ExecuteStatementRequest
}

// Statement returns the InfluxQL statement.
func (r *ExecuteStatementRequest) Statement() string { return r.pb.GetStatement() }

// SetStatement sets the InfluxQL statement.
func (r *ExecuteStatementRequest) SetStatement(statement string) {
	r.pb.Statement = proto.String(statement)
}

// Database returns the database name.
func (r *ExecuteStatementRequest) Database() string { return r.pb.GetDatabase() }

// SetDatabase sets the database name.
func (r *ExecuteStatementRequest) SetDatabase(database string) { r.pb.Database = proto.String(database) }

// MarshalBinary encodes the object to a binary format.
func (r *ExecuteStatementRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.pb)
}

// UnmarshalBinary populates ExecuteStatementRequest from a binary format.
func (r *ExecuteStatementRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &r.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementResponse represents the response returned from a remote ExecuteStatementRequest call.
type ExecuteStatementResponse struct {
	pb internal.WriteShardResponse
}

// Code returns the response code.
func (w *ExecuteStatementResponse) Code() int { return int(w.pb.GetCode()) }

// SetCode sets the Code
func (w *ExecuteStatementResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// Message returns the repsonse message.
func (w *ExecuteStatementResponse) Message() string { return w.pb.GetMessage() }

// SetMessage sets the Message
func (w *ExecuteStatementResponse) SetMessage(message string) { w.pb.Message = &message }

// MarshalBinary encodes the object to a binary format.
func (w *ExecuteStatementResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates ExecuteStatementResponse from a binary format.
func (w *ExecuteStatementResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// CreateIteratorRequest represents a request to create a remote iterator.
type CreateIteratorRequest struct {
	ShardIDs []uint64
	Opt      influxql.IteratorOptions
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.CreateIteratorRequest{
		ShardIDs: r.ShardIDs,
		Opt:      buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

// CreateIteratorResponse represents a response from remote iterator creation.
type CreateIteratorResponse struct {
	Err error
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CreateIteratorResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// FieldDimensionsRequest represents a request to retrieve unique fields & dimensions.
type FieldDimensionsRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.FieldDimensionsRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsRequest) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// FieldDimensionsResponse represents a response from remote iterator creation.
type FieldDimensionsResponse struct {
	Fields     map[string]struct{}
	Dimensions map[string]struct{}
	Err        error
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsResponse) MarshalBinary() ([]byte, error) {
	var pb internal.FieldDimensionsResponse

	pb.Fields = make([]string, 0, len(r.Fields))
	for k := range r.Fields {
		pb.Fields = append(pb.Fields, k)
	}

	pb.Dimensions = make([]string, 0, len(r.Dimensions))
	for k := range r.Dimensions {
		pb.Dimensions = append(pb.Dimensions, k)
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsResponse) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.Fields = make(map[string]struct{}, len(pb.GetFields()))
	for _, s := range pb.GetFields() {
		r.Fields[s] = struct{}{}
	}

	r.Dimensions = make(map[string]struct{}, len(pb.GetDimensions()))
	for _, s := range pb.GetDimensions() {
		r.Dimensions[s] = struct{}{}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// SeriesKeysRequest represents a request to retrieve a list of series keys.
type SeriesKeysRequest struct {
	ShardIDs []uint64
	Opt      influxql.IteratorOptions
}

// MarshalBinary encodes r to a binary format.
func (r *SeriesKeysRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.SeriesKeysRequest{
		ShardIDs: r.ShardIDs,
		Opt:      buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *SeriesKeysRequest) UnmarshalBinary(data []byte) error {
	var pb internal.SeriesKeysRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

// SeriesKeysResponse represents a response from retrieving series keys.
type SeriesKeysResponse struct {
	SeriesList influxql.SeriesList
	Err        error
}

// MarshalBinary encodes r to a binary format.
func (r *SeriesKeysResponse) MarshalBinary() ([]byte, error) {
	var pb internal.SeriesKeysResponse

	buf, err := r.SeriesList.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pb.SeriesList = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *SeriesKeysResponse) UnmarshalBinary(data []byte) error {
	var pb internal.SeriesKeysResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if err := r.SeriesList.UnmarshalBinary(pb.GetSeriesList()); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}

	return nil
}

// ExpandSourcesRequest represents a request to expand regex sources.
type ExpandSourcesRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.ExpandSourcesRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// ExpandSourcesResponse represents a response from source expansion.
type ExpandSourcesResponse struct {
	Sources influxql.Sources
	Err     error
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.ExpandSourcesResponse
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pb.Sources = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}
