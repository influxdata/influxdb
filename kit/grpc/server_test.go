package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	testpb "github.com/influxdata/influxdb/kit/grpc/internal"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	"go.uber.org/zap/zaptest"

	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"google.golang.org/grpc"
)

//go:generate protoc --go_out=plugins=grpc:. kit/grpc/internal/test.proto

func TestNewServer(t *testing.T) {
	sampler := jaeger.NewConstSampler(true)
	reporter := jaeger.NewInMemoryReporter()
	tracer, closer := jaeger.NewTracer("service name", sampler, reporter)
	defer closer.Close()
	oldTracer := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	defer opentracing.SetGlobalTracer(oldTracer)

	srv, metrics := NewServer()
	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(metrics...)

	var (
		method string
		ok     bool
	)

	ss := &stubServer{
		unaryCall: func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			method, ok = grpc.Method(ctx)
			return &testpb.SimpleResponse{}, nil
		},
		streamingOutputCall: func(in *testpb.StreamingOutputCallRequest, stream testpb.TestService_StreamingOutputCallServer) error {
			method, ok = grpc.Method(stream.Context())
			if err := stream.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
				t.Errorf("stream.Send(_)= %v, want <nil>", err)
				return err
			}
			return nil
		},
	}
	if err := ss.Start(srv); err != nil {
		t.Fatalf("error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	span, ctx := tracing.StartSpanFromContext(context.Background())
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := ss.client.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
		t.Errorf("ss.client.UnaryCall(_, _) = _, %v; want _, nil", err)
	}
	if want := "/internal.TestService/UnaryCall"; !ok || method != want {
		t.Errorf("grpc.Method(_) = %q, %v; want %q, true", method, ok, want)
	}

	stream, err := ss.client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Errorf("ss.client.StreamingOutputCall(_, _) = _, %v; want _, nil", err)
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("ss.client.StreamingOutputCall(_, _).Recv() = _, %v; want _, nil", err)
		}
	}

	if want := "/internal.TestService/StreamingOutputCall"; !ok || method != want {
		t.Errorf("grpc.Method(_) = %q, %v; want %q, true", method, ok, want)
	}

	span.Finish()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("unable to gather prom metrics: %v", err)
	}

	c := promtest.MustFindMetric(t, mfs,
		"grpc_server_handled_total",
		map[string]string{
			"grpc_code":    "OK",
			"grpc_method":  "UnaryCall",
			"grpc_service": "internal.TestService",
			"grpc_type":    "unary",
		},
	)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Errorf("expected counter to be 1, got %v", got)
	}

	c = promtest.MustFindMetric(t, mfs,
		"grpc_server_msg_received_total",
		map[string]string{
			"grpc_method":  "UnaryCall",
			"grpc_service": "internal.TestService",
			"grpc_type":    "unary",
		},
	)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Errorf("expected counter to be 1, got %v", got)
	}

	c = promtest.MustFindMetric(t, mfs,
		"grpc_server_msg_sent_total",
		map[string]string{
			"grpc_method":  "UnaryCall",
			"grpc_service": "internal.TestService",
			"grpc_type":    "unary",
		},
	)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Errorf("expected counter to be 1, got %v", got)
	}

	h := promtest.MustFindMetric(t, mfs,
		"grpc_server_handling_seconds",
		map[string]string{
			"grpc_method":  "UnaryCall",
			"grpc_service": "internal.TestService",
			"grpc_type":    "unary",
		},
	)
	if got := h.GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("expected histogram to be 1, got %v", got)
	}

	c = promtest.MustFindMetric(t, mfs,
		"grpc_server_handled_total",
		map[string]string{
			"grpc_code":    "OK",
			"grpc_method":  "StreamingOutputCall",
			"grpc_service": "internal.TestService",
			"grpc_type":    "server_stream",
		},
	)
	if got := c.GetCounter().GetValue(); got != 1 {
		t.Errorf("expected counter to be 1, got %v", got)
	}

	/*
			There are 5 total spans; one that starts within this test itself,
			followed by pairs for each client/server call.

		           +----------+
		           | TestSpan |
		           +----+-----+
		                |
		        v-------+--------v
		+--------------+  +---------------+
		| Unary Client |  | Stream Client |
		+-------+------+  +--------+------+
		        |                  |
		+-------v------+  +--------v------+
		| Unary Server |  | Stream Server |
		+--------------+  +---------------+
	*/

	if got := reporter.SpansSubmitted(); got != 5 {
		t.Errorf("expected 5 span, got %v", got)
		spans := reporter.GetSpans()
		for i := range spans {
			spanCtx := spans[i].Context().(jaeger.SpanContext)
			span := spans[i].(*jaeger.Span)
			name := span.OperationName()
			t.Logf("%s %v %v", name, spans[i], spanCtx)
		}
	}
}

// stubServer is a server that is easy to customize within individual test
// cases.
//
// copied from https://github.com/grpc/grpc-go
type stubServer struct {
	// Guarantees we satisfy this interface; panics if unimplemented methods are called.
	testpb.TestServiceServer

	// Customizable implementations of server handlers.
	unaryCall           func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error)
	streamingOutputCall func(in *testpb.StreamingOutputCallRequest, stream testpb.TestService_StreamingOutputCallServer) error

	// A client connected to this service the test may use.  Created in Start().
	client testpb.TestServiceClient
	cc     *grpc.ClientConn
	s      *grpc.Server

	addr     string   // address of listener
	cleanups []func() // Lambdas executed in Stop(); populated by Start().
}

func (ss *stubServer) UnaryCall(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return ss.unaryCall(ctx, in)
}

func (ss *stubServer) StreamingOutputCall(in *testpb.StreamingOutputCallRequest, stream testpb.TestService_StreamingOutputCallServer) error {
	return ss.streamingOutputCall(in, stream)
}

// Start starts the server and creates a client connected to it.
func (ss *stubServer) Start(s *grpc.Server) error {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return fmt.Errorf(`net.Listen("tcp", "localhost:0") = %v`, err)
	}
	ss.addr = lis.Addr().String()
	ss.cleanups = append(ss.cleanups, func() { lis.Close() })

	testpb.RegisterTestServiceServer(s, ss)
	go s.Serve(lis)
	ss.cleanups = append(ss.cleanups, s.Stop)
	ss.s = s

	target := ss.addr

	tracer := opentracing.GlobalTracer()
	cc, err := grpc.Dial(
		target,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(otgrpc.OpenTracingClientInterceptor(tracer)),
		grpc.WithStreamInterceptor(otgrpc.OpenTracingStreamClientInterceptor(tracer)),
	)
	if err != nil {
		return fmt.Errorf("grpc.Dial(%q) = %v", target, err)
	}
	ss.cc = cc
	ss.cleanups = append(ss.cleanups, func() { cc.Close() })
	ss.client = testpb.NewTestServiceClient(cc)
	return nil
}

func (ss *stubServer) Stop() {
	for i := len(ss.cleanups) - 1; i >= 0; i-- {
		ss.cleanups[i]()
	}
}
