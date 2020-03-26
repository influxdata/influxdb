package grpc

import (
	"context"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// NewServer creates a gRPC server that registers both opentracing
// and prometheus metrics for gRPC server requests.
func NewServer() (*grpc.Server, []prometheus.Collector) {
	tracer := opentracing.GlobalTracer()
	grpcMetrics := grpc_prometheus.NewServerMetrics()

	// according to prometheus folks, adding histograms for each
	// gRPC request message type drives up cardinality a lot.  If this
	// is too problematic, we can change the bucket count.
	grpcMetrics.EnableHandlingTimeHistogram()

	s := grpc.NewServer(
		grpc.UnaryInterceptor(
			chainUnaryServer(
				grpcMetrics.UnaryServerInterceptor(),
				otgrpc.OpenTracingServerInterceptor(tracer),
			),
		),
		grpc.StreamInterceptor(
			chainStreamServer(
				grpcMetrics.StreamServerInterceptor(),
				otgrpc.OpenTracingStreamServerInterceptor(tracer),
			),
		),
	)

	grpcMetrics.InitializeMetrics(s)

	return s, []prometheus.Collector{grpcMetrics}
}

// chainUnaryServer creates a single server-side interceptor out of a chain of
// several.
//
// This is copied from:
// https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/chain.go
//
// Here is the original comment:
//
// ChainUnaryServer creates a single interceptor out of a chain of many interceptors.
//
// Execution is done in left-to-right order, including passing of context.
// For example ChainUnaryServer(one, two, three) will execute one before two before three, and three
// will see context changes of one and two.
func chainUnaryServer(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	n := len(interceptors)

	if n > 1 {
		lastI := n - 1
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			var (
				chainHandler grpc.UnaryHandler
				curI         int
			)

			chainHandler = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				if curI == lastI {
					return handler(currentCtx, currentReq)
				}
				curI++
				resp, err := interceptors[curI](currentCtx, currentReq, info, chainHandler)
				curI--
				return resp, err
			}

			return interceptors[0](ctx, req, info, chainHandler)
		}
	}

	if n == 1 {
		return interceptors[0]
	}

	// n == 0; Dummy interceptor maintained for backward compatibility to avoid returning nil.
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
}

// chainStreamServer creates a single server-side interceptor out of a chain of
// several.
//
// This is copied from:
// https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/chain.go
//
// Here is the original comment:
// chainStreamServer creates a single interceptor out of a chain of many interceptors.
//
// Execution is done in left-to-right order, including passing of context.
// For example ChainUnaryServer(one, two, three) will execute one before two before three.
// If you want to pass context between interceptors, use WrapServerStream.
func chainStreamServer(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	n := len(interceptors)

	if n > 1 {
		lastI := n - 1
		return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			var (
				chainHandler grpc.StreamHandler
				curI         int
			)

			chainHandler = func(currentSrv interface{}, currentStream grpc.ServerStream) error {
				if curI == lastI {
					return handler(currentSrv, currentStream)
				}
				curI++
				err := interceptors[curI](currentSrv, currentStream, info, chainHandler)
				curI--
				return err
			}

			return interceptors[0](srv, stream, info, chainHandler)
		}
	}

	if n == 1 {
		return interceptors[0]
	}

	// n == 0; Dummy interceptor maintained for backward compatibility to avoid returning nil.
	return func(srv interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, stream)
	}
}
