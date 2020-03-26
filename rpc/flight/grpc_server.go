package flight

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	ipc2 "github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gogo/protobuf/types"
	igrpc "github.com/influxdata/influxdb/kit/grpc"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/rpc/flight/ipc"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	ErrNotImplemented = errors.New("not implemented")
)

type storeGRPCServer struct {
	loggingEnabled bool
	rpc            *grpc.Server
	store          reads.Store
	query          *control.Controller
	logger         *zap.Logger
	collectors     []prometheus.Collector
}

func (s *storeGRPCServer) Serve(ln net.Listener) error {
	s.rpc, s.collectors = igrpc.NewServer()

	ipc.RegisterFlightServiceServer(s.rpc, s)

	s.logger.Info("Apache Flight gRPC service listening", zap.String("address", ln.Addr().String()))
	return s.rpc.Serve(ln)
}

func (s *storeGRPCServer) Shutdown(ctx context.Context) {
	s.rpc.GracefulStop()
}

func (s *storeGRPCServer) WithLogger(log *zap.Logger) {
	s.logger = log
}

func (s *storeGRPCServer) PrometheusCollectors() []prometheus.Collector {
	return s.collectors
}

// FlightServiceServer

func (s *storeGRPCServer) Handshake(server ipc.FlightService_HandshakeServer) error {
	return ErrNotImplemented
}

func (s *storeGRPCServer) ListFlights(criteria *ipc.Criteria, server ipc.FlightService_ListFlightsServer) error {
	return ErrNotImplemented
}

func (s *storeGRPCServer) GetFlightInfo(ctx context.Context, descriptor *ipc.FlightDescriptor) (*ipc.FlightInfo, error) {
	return nil, ErrNotImplemented
}

var (
	timestampNS = &arrow.Time64Type{Unit: arrow.Nanosecond}
)

func (s *storeGRPCServer) getSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
}

func (s *storeGRPCServer) GetSchema(ctx context.Context, descriptor *ipc.FlightDescriptor) (*ipc.SchemaResult, error) {
	if descriptor.Type != ipc.FlightDescriptor_CMD {
		return nil, errors.New("only CMD descriptor supported")
	}

	var gr GetRequest
	err := json.Unmarshal(descriptor.Cmd, &gr)
	if err != nil {
		return nil, err
	}

	req, err := s.makeReadGroupRequest(&gr)
	if err != nil {
		return nil, err
	}

	schema, err := s.getSchemaForRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	wr := ipc2.NewWriter(&buf, ipc2.WithSchema(schema))
	wr.Close()

	return &ipc.SchemaResult{Schema: buf.Bytes()}, nil
}

type GetRequest struct {
	OrgID       ID        `json:"org_id"`
	BucketID    ID        `json:"bucket_id"`
	Measurement string    `json:"measurement"`
	Field       string    `json:"field"`
	Start       time.Time `json:"start"`
	Stop        time.Time `json:"stop"`
}

func (s *storeGRPCServer) makeReadGroupRequest(gr *GetRequest) (*datatypes.ReadGroupRequest, error) {
	var startTime = models.MinNanoTime
	if !gr.Start.IsZero() {
		startTime = gr.Start.UnixNano()
	}

	var endTime = models.MaxNanoTime
	if !gr.Stop.IsZero() {
		endTime = gr.Start.UnixNano()
	}

	items := make([]string, 0, 2)
	if len(gr.Measurement) > 0 {
		items = append(items, fmt.Sprintf("_m = '%s'", gr.Measurement))
	}
	if len(gr.Field) > 0 {
		items = append(items, fmt.Sprintf("_f = '%s'", gr.Field))
	}

	exprStr := "(" + strings.Join(items, " AND ") + ")"
	expr, err := influxql.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}
	var v exprToNodeVisitor
	influxql.Walk(&v, expr)
	if err := v.Err(); err != nil {
		return nil, err
	}
	predicate := datatypes.Predicate{Root: v.nodes[0]}

	// Setup read request
	any, err := types.MarshalAny(s.store.GetSource(uint64(gr.OrgID), uint64(gr.BucketID)))
	if err != nil {
		return nil, err
	}

	return &datatypes.ReadGroupRequest{
		ReadSource: any,
		Range: datatypes.TimestampRange{
			Start: startTime,
			End:   endTime,
		},
		Predicate: &predicate,
		Group:     datatypes.GroupNone,
	}, nil
}

func (s *storeGRPCServer) DoGet(ticket *ipc.Ticket, server ipc.FlightService_DoGetServer) error {
	var gr GetRequest
	err := json.Unmarshal(ticket.Ticket, &gr)
	if err != nil {
		return err
	}

	req, err := s.makeReadGroupRequest(&gr)
	if err != nil {
		return err
	}

	ctx := server.Context()

	schema, err := s.getSchemaForRequest(ctx, req)
	if err != nil {
		return err
	}

	grs, err := s.store.ReadGroup(ctx, req)
	if err != nil {
		return err
	}
	defer grs.Close()

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	wr := ipc.NewWriter(server, ipc.WithSchema(schema), ipc.WithAllocator(pool))

	for gc := grs.Next(); gc != nil; gc = grs.Next() {
		for gc.Next() {
			cur := gc.Cursor()
			if cur == nil {
				continue
			}

			rows := 0

			t64B := b.Field(0).(*array.Int64Builder)
			i64B := b.Field(2).(*array.Int64Builder)
			f64B := b.Field(3).(*array.Float64Builder)

			switch cur := cur.(type) {
			case cursors.FloatArrayCursor:
				for a := cur.Next(); a.Len() > 0; a = cur.Next() {
					t64B.AppendValues(a.Timestamps, nil)
					f64B.AppendValues(a.Values, nil)
					rows += a.Len()
					for i := 0; i < a.Len(); i++ {
						i64B.AppendNull()
					}
				}

			case cursors.IntegerArrayCursor:
				for a := cur.Next(); a.Len() > 0; a = cur.Next() {
					t64B.AppendValues(a.Timestamps, nil)
					i64B.AppendValues(a.Values, nil)
					rows += a.Len()
					for i := 0; i < a.Len(); i++ {
						f64B.AppendNull()
					}
				}
			}

			cur.Close()

			rowTags := gc.Tags()

			keyB := b.Field(1).(*array.StringBuilder)
			keyV := string(rowTags[2:].HashKey())
			for i := 0; i < rows; i++ {
				keyB.Append(keyV)
			}

			fields := schema.Fields()[4:]
			builders := b.Fields()[4:]

			for i := range fields {
				f := &fields[i]
				sb := builders[i].(*array.StringBuilder)

				val := rowTags.GetString(f.Name)
				if len(val) == 0 {
					for i := 0; i < rows; i++ {
						sb.AppendNull()
					}
				} else {
					for i := 0; i < rows; i++ {
						sb.Append(val)
					}
				}
			}

			rec := b.NewRecord()
			_ = wr.Write(rec)
			rec.Release()
		}
		gc.Close()
	}

	_ = wr.Close()

	return nil
}

func (s *storeGRPCServer) getSchemaForRequest(ctx context.Context, req *datatypes.ReadGroupRequest) (*arrow.Schema, error) {
	grs, err := s.store.ReadGroup(ctx, req)
	if err != nil {
		return nil, err
	}
	defer grs.Close()

	fields := []arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "key", Type: arrow.BinaryTypes.String},
		{Name: "value_i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "value_f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}

	gc := grs.Next()
	keys := gc.Keys()
	for _, key := range keys {
		fields = append(fields, arrow.Field{
			Name:     string(key),
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		})
	}
	gc.Close()

	return arrow.NewSchema(fields, nil), nil
}

func (s *storeGRPCServer) DoPut(server ipc.FlightService_DoPutServer) error {
	return ErrNotImplemented
}

func (s *storeGRPCServer) DoAction(action *ipc.Action, server ipc.FlightService_DoActionServer) error {
	return ErrNotImplemented
}

func (s *storeGRPCServer) ListActions(empty *ipc.Empty, server ipc.FlightService_ListActionsServer) error {
	return ErrNotImplemented
}
