package control_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/mock"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/plan/plantest"
	"github.com/influxdata/flux/stdlib/universe"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/feature"
	pmock "github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap/zaptest"
)

func init() {
	execute.RegisterSource(executetest.AllocatingFromTestKind, executetest.CreateAllocatingFromSource)
}

var (
	mockCompiler = &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					q.ResultsCh <- &executetest.Result{}
				},
			}, nil
		},
	}
	config = control.Config{
		MemoryBytesQuotaPerQuery: math.MaxInt64,
	}
	limitedConfig = control.Config{
		MemoryBytesQuotaPerQuery: math.MaxInt64,
		ConcurrencyQuota:         1,
		QueueSize:                1,
	}
	bothConfigs          = map[string]control.Config{"unlimited": config, "limited": limitedConfig}
)

func setupPromRegistry(c *control.Controller) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	for _, col := range c.PrometheusCollectors() {
		err := reg.Register(col)
		if err != nil {
			panic(err)
		}
	}
	return reg
}

func validateRequestTotals(t testing.TB, reg *prometheus.Registry, success, compile, runtime, queue int) {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	validate := func(name string, want int) {
		m := FindMetric(
			metrics,
			"qc_requests_total",
			map[string]string{
				"result": name,
				"org":    "",
			},
		)
		var got int
		if m != nil {
			got = int(*m.Counter.Value)
		}
		if got != want {
			t.Errorf("unexpected %s total: got %d want: %d", name, got, want)
		}
	}
	validate("success", success)
	validate("compile_error", compile)
	validate("runtime_error", runtime)
	validate("queue_error", queue)
}

func validateUnusedMemory(t testing.TB, reg *prometheus.Registry, c control.Config) {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	m := FindMetric(
		metrics,
		"qc_memory_unused_bytes",
		map[string]string{
			"org": "",
		},
	)
	var got int64
	if m != nil {
		got = int64(*m.Gauge.Value)
	}
	want := c.MaxMemoryBytes - (int64(c.ConcurrencyQuota) * c.InitialMemoryBytesQuotaPerQuery)
	if got != want {
		t.Errorf("unexpected memory unused bytes: got %d want: %d", got, want)
	}
}

func TestController_QuerySuccess(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			reg := setupPromRegistry(ctrl)

			q, err := ctrl.Query(context.Background(), makeRequest(mockCompiler))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			for range q.Results() {
				// discard the results as we do not care.
			}
			q.Done()

			if err := q.Err(); err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			stats := q.Statistics()
			if stats.CompileDuration == 0 {
				t.Error("expected compile duration to be above zero")
			}
			if stats.QueueDuration == 0 {
				t.Error("expected queue duration to be above zero")
			}
			if stats.ExecuteDuration == 0 {
				t.Error("expected execute duration to be above zero")
			}
			if stats.TotalDuration == 0 {
				t.Error("expected total duration to be above zero")
			}
			validateRequestTotals(t, reg, 1, 0, 0, 0)
		})
	}
}

func TestController_QueryCompileError(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			reg := setupPromRegistry(ctrl)

			q, err := ctrl.Query(context.Background(), makeRequest(&mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return nil, errors.New("compile error")
				},
			}))
			if err == nil {
				t.Error("expected compiler error")
			}

			if q != nil {
				t.Errorf("unexpected query value: %v", q)
				defer q.Done()
			}

			validateRequestTotals(t, reg, 0, 1, 0, 0)
		})
	}
}

func TestController_QueryRuntimeError(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			reg := setupPromRegistry(ctrl)

			q, err := ctrl.Query(context.Background(), makeRequest(&mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
							q.SetErr(errors.New("runtime error"))
						},
					}, nil
				},
			}))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			for range q.Results() {
				// discard the results as we do not care.
			}
			q.Done()

			if q.Err() == nil {
				t.Error("expected runtime error")
			}

			stats := q.Statistics()
			if stats.CompileDuration == 0 {
				t.Error("expected compile duration to be above zero")
			}
			if stats.QueueDuration == 0 {
				t.Error("expected queue duration to be above zero")
			}
			if stats.ExecuteDuration == 0 {
				t.Error("expected execute duration to be above zero")
			}
			if stats.TotalDuration == 0 {
				t.Error("expected total duration to be above zero")
			}
			validateRequestTotals(t, reg, 0, 0, 1, 0)
		})
	}
}

func TestController_QueryQueueError(t *testing.T) {
	t.Skip("This test exposed several race conditions, its not clear if the races are specific to the test case")

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	reg := setupPromRegistry(ctrl)

	// This channel blocks program execution until we are done
	// with running the test.
	done := make(chan struct{})
	defer close(done)

	// Insert three queries, two that block forever and a last that does not.
	// The third should error to be enqueued.
	for i := 0; i < 2; i++ {
		q, err := ctrl.Query(context.Background(), makeRequest(&mock.Compiler{
			CompileFn: func(ctx context.Context) (flux.Program, error) {
				return &mock.Program{
					ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
						// Block until test is finished
						<-done
					},
				}, nil
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer q.Done()
	}

	// Third "normal" query
	q, err := ctrl.Query(context.Background(), makeRequest(mockCompiler))
	if err == nil {
		t.Error("expected queue error")
	}

	if q != nil {
		t.Errorf("unexpected query value: %v", q)
		defer q.Done()
	}

	validateRequestTotals(t, reg, 0, 0, 0, 1)
}

// TODO(nathanielc): Use promtest in influxdb/kit

// FindMetric iterates through mfs to find the first metric family matching name.
// If a metric family matches, then the metrics inside the family are searched,
// and the first metric whose labels match the given labels are returned.
// If no matches are found, FindMetric returns nil.
//
// FindMetric assumes that the labels on the metric family are well formed,
// i.e. there are no duplicate label names, and the label values are not empty strings.
func FindMetric(mfs []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	_, m := findMetric(mfs, name, labels)
	return m
}

// findMetric is a helper that returns the matching family and the matching metric.
// The exported FindMetric function specifically only finds the metric, not the family,
// but for test it is more helpful to identify whether the family was matched.
func findMetric(mfs []*dto.MetricFamily, name string, labels map[string]string) (*dto.MetricFamily, *dto.Metric) {
	var fam *dto.MetricFamily

	for _, mf := range mfs {
		if mf.GetName() == name {
			fam = mf
			break
		}
	}

	if fam == nil {
		// No family matching the name.
		return nil, nil
	}

	for _, m := range fam.Metric {
		if len(m.Label) != len(labels) {
			continue
		}

		match := true
		for _, l := range m.Label {
			if labels[l.GetName()] != l.GetValue() {
				match = false
				break
			}
		}

		if !match {
			continue
		}

		// All labels matched.
		return fam, m
	}

	// Didn't find a metric whose labels all matched.
	return fam, nil
}

func TestController_AfterShutdown(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			shutdown(t, ctrl)

			// No point in continuing. The shutdown didn't work
			// even though there are no queries.
			if t.Failed() {
				return
			}

			if _, err := ctrl.Query(context.Background(), makeRequest(mockCompiler)); err == nil {
				t.Error("expected error")
			} else if got, want := err.Error(), "query controller shutdown"; got != want {
				t.Errorf("unexpected error -want/+got\n\t- %q\n\t+ %q", want, got)
			}
		})
	}
}

func TestController_CompileError(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return nil, &flux.Error{
						Code: codes.Invalid,
						Msg:  "expected error",
					}
				},
			}
			if _, err := ctrl.Query(context.Background(), makeRequest(compiler)); err == nil {
				t.Error("expected error")
			} else if got, want := err.Error(), "compilation failed: expected error"; got != want {
				t.Errorf("unexpected error -want/+got\n\t- %q\n\t+ %q", want, got)
			}
		})
	}
}

func TestController_ExecuteError(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						StartFn: func(ctx context.Context, alloc *memory.Allocator) (*mock.Query, error) {
							return nil, errors.New("expected error")
						},
					}, nil
				},
			}

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			// There should be no results.
			numResults := 0
			for range q.Results() {
				numResults++
			}

			if numResults != 0 {
				t.Errorf("no results should have been returned, but %d were", numResults)
			}
			q.Done()

			if err := q.Err(); err == nil {
				t.Error("expected error")
			} else if got, want := err.Error(), "expected error"; got != want {
				t.Errorf("unexpected error -want/+got\n\t- %q\n\t+ %q", want, got)
			}
		})
	}
}

func TestController_LimitExceededError(t *testing.T) {

	const memoryBytesQuotaPerQuery = 64
	config := config
	config.MemoryBytesQuotaPerQuery = memoryBytesQuotaPerQuery
	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			// Return a program that will allocate one more byte than is allowed.
			pts := plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("allocating-from-test", &executetest.AllocatingFromProcedureSpec{
						ByteCount: memoryBytesQuotaPerQuery + 1,
					}),
					plan.CreatePhysicalNode("yield", &universe.YieldProcedureSpec{Name: "_result"}),
				},
				Edges: [][2]int{
					{0, 1},
				},
				Resources: flux.ResourceManagement{
					ConcurrencyQuota: 1,
				},
			}

			ps := plantest.CreatePlanSpec(&pts)
			prog := &lang.Program{
				Logger:   zaptest.NewLogger(t),
				PlanSpec: ps,
			}

			return prog, nil
		},
	}

	q, err := ctrl.Query(context.Background(), makeRequest(compiler))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	ri := flux.NewResultIteratorFromQuery(q)
	defer ri.Release()
	for ri.More() {
		res := ri.Next()
		err = res.Tables().Do(func(t flux.Table) error {
			return nil
		})
		if err != nil {
			break
		}
	}
	ri.Release()

	if err == nil {
		t.Fatal("expected an error")
	}

	if !strings.Contains(err.Error(), "memory") {
		t.Fatalf("expected an error about memory limit exceeded, got %v", err)
	}

	stats := ri.Statistics()
	if len(stats.RuntimeErrors) != 1 {
		t.Fatal("expected one runtime error reported in stats")
	}

	if !strings.Contains(stats.RuntimeErrors[0], "memory") {
		t.Fatalf("expected an error about memory limit exceeded, got %v", err)
	}
}

func TestController_CompilePanic(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					panic("panic during compile step")
				},
			}

			_, err = ctrl.Query(context.Background(), makeRequest(compiler))
			if err == nil {
				t.Fatalf("expected error when query was compiled")
			} else if !strings.Contains(err.Error(), "panic during compile step") {
				t.Fatalf(`expected error to contain "panic during compile step" instead it contains "%v"`, err.Error())
			}
		})
	}
}

func TestController_StartPanic(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						StartFn: func(ctx context.Context, alloc *memory.Allocator) (i *mock.Query, e error) {
							panic("panic during start step")
						},
					}, nil
				},
			}

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Fatalf("unexpected error when query was compiled")
			}

			for range q.Results() {
			}
			q.Done()

			if err = q.Err(); err == nil {
				t.Fatalf("expected error after query started")
			} else if !strings.Contains(err.Error(), "panic during start step") {
				t.Fatalf(`expected error to contain "panic during start step" instead it contains "%v"`, err.Error())
			}
		})
	}
}

func TestController_ShutdownWithRunningQuery(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			executing := make(chan struct{})
			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
							close(executing)
							<-ctx.Done()

							// This should still be read even if we have been canceled.
							q.ResultsCh <- &executetest.Result{}
						},
					}, nil
				},
			}

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range q.Results() {
					// discard the results
				}
				q.Done()
			}()

			// Wait until execution has started.
			<-executing

			// Shutdown should succeed and not timeout. The above blocked
			// query should be canceled and then shutdown should return.
			shutdown(t, ctrl)
			wg.Wait()
		})
	}
}

func TestController_ShutdownWithTimeout(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			// This channel blocks program execution until we are done
			// with running the test.
			done := make(chan struct{})
			defer close(done)

			executing := make(chan struct{})
			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
							// This should just block until the end of the test
							// when we perform cleanup.
							close(executing)
							<-done
						},
					}, nil
				},
			}

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			go func() {
				for range q.Results() {
					// discard the results
				}
				q.Done()
			}()

			// Wait until execution has started.
			<-executing

			// The shutdown should not succeed.
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			if err := ctrl.Shutdown(ctx); err == nil {
				t.Error("expected error")
			} else if got, want := err.Error(), context.DeadlineExceeded.Error(); got != want {
				t.Errorf("unexpected error -want/+got\n\t- %q\n\t+ %q", want, got)
			}
			cancel()
		})
	}
}

func TestController_PerQueryMemoryLimit(t *testing.T) {
	for name, config := range bothConfigs {
		t.Run(name, func(t *testing.T) {
			ctrl, err := control.New(config, zaptest.NewLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, ctrl)

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					return &mock.Program{
						ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
							defer func() {
								if err, ok := recover().(error); ok && err != nil {
									q.SetErr(err)
								}
							}()

							// This is emulating the behavior of exceeding the memory limit at runtime
							mem := arrow.NewAllocator(alloc)
							b := mem.Allocate(int(config.MemoryBytesQuotaPerQuery + 1))
							mem.Free(b)
						},
					}, nil
				},
			}

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Fatal(err)
			}

			for range q.Results() {
				// discard the results
			}
			q.Done()

			if q.Err() == nil {
				t.Fatal("expected error about memory limit exceeded")
			}
		})
	}
}

func TestController_ConcurrencyQuota(t *testing.T) {
	const (
		numQueries       = 3
		concurrencyQuota = 2
	)

	config := config
	config.ConcurrencyQuota = concurrencyQuota
	config.QueueSize = numQueries
	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	executing := make(chan struct{}, numQueries)
	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					select {
					case <-q.Canceled:
					default:
						executing <- struct{}{}
						<-q.Canceled
					}
				},
			}, nil
		},
	}

	for i := 0; i < numQueries; i++ {
		q, err := ctrl.Query(context.Background(), makeRequest(compiler))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for range q.Results() {
				// discard the results
			}
			q.Done()
		}()
	}

	// Give 2 queries a chance to begin executing.  The remaining third query should stay queued.
	time.Sleep(250 * time.Millisecond)

	if err := ctrl.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}

	// There is a chance that the remaining query managed to get executed after the executing queries
	// were canceled.  As a result, this test is somewhat flaky.

	close(executing)

	var count int
	for range executing {
		count++
	}

	if count != concurrencyQuota {
		t.Fatalf("expected exactly %v queries to execute, but got: %v", concurrencyQuota, count)
	}
}

func TestController_QueueSize(t *testing.T) {
	const (
		concurrencyQuota = 2
		queueSize        = 3
	)

	config := config
	config.ConcurrencyQuota = concurrencyQuota
	config.QueueSize = queueSize
	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	// This channel blocks program execution until we are done
	// with running the test.
	done := make(chan struct{})
	defer close(done)

	executing := make(chan struct{}, concurrencyQuota+queueSize)
	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					executing <- struct{}{}
					// Block until test is finished
					<-done
				},
			}, nil
		},
	}

	// Start as many queries as can be running at the same time
	for i := 0; i < concurrencyQuota; i++ {
		q, err := ctrl.Query(context.Background(), makeRequest(compiler))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for range q.Results() {
				// discard the results
			}
			q.Done()
		}()

		// Wait until it's executing
		<-executing
	}

	// Now fill up the queue
	for i := 0; i < queueSize; i++ {
		q, err := ctrl.Query(context.Background(), makeRequest(compiler))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for range q.Results() {
				// discard the results
			}
			q.Done()
		}()
	}

	_, err = ctrl.Query(context.Background(), makeRequest(compiler))
	if err == nil {
		t.Fatal("expected an error about queue length exceeded")
	}
}

// Test that rapidly starting and canceling the query and then calling done will correctly
// cancel the query and not result in a race condition.
func TestController_CancelDone_Unlimited(t *testing.T) {
	config := config

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Ensure the query takes a little bit of time so the cancel actually cancels something.
					t := time.NewTimer(time.Second)
					defer t.Stop()

					select {
					case <-t.C:
					case <-ctx.Done():
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
				return
			}
			q.Cancel()
			q.Done()
		}()
	}
	wg.Wait()
}

// Test that rapidly starts and calls done on queries without reading the result.
func TestController_DoneWithoutRead_Unlimited(t *testing.T) {
	config := config

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Ensure the query takes a little bit of time so the cancel actually cancels something.
					t := time.NewTimer(time.Second)
					defer t.Stop()

					select {
					case <-t.C:
						q.ResultsCh <- &executetest.Result{
							Nm:   "_result",
							Tbls: []*executetest.Table{},
						}
					case <-ctx.Done():
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
				return
			}
			// If we call done without reading anything it should work just fine.
			q.Done()
		}()
	}
	wg.Wait()
}

// Test that rapidly starting and canceling the query and then calling done will correctly
// cancel the query and not result in a race condition.
func TestController_CancelDone(t *testing.T) {
	config := config
	config.ConcurrencyQuota = 10
	config.QueueSize = 200

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Ensure the query takes a little bit of time so the cancel actually cancels something.
					t := time.NewTimer(time.Second)
					defer t.Stop()

					select {
					case <-t.C:
					case <-ctx.Done():
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
				return
			}
			q.Cancel()
			q.Done()
		}()
	}
	wg.Wait()
}

// Test that rapidly starts and calls done on queries without reading the result.
func TestController_DoneWithoutRead(t *testing.T) {
	config := config
	config.ConcurrencyQuota = 10
	config.QueueSize = 200

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Ensure the query takes a little bit of time so the cancel actually cancels something.
					t := time.NewTimer(time.Second)
					defer t.Stop()

					select {
					case <-t.C:
						q.ResultsCh <- &executetest.Result{
							Nm:   "_result",
							Tbls: []*executetest.Table{},
						}
					case <-ctx.Done():
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			q, err := ctrl.Query(context.Background(), makeRequest(compiler))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
				return
			}
			// If we call done without reading anything it should work just fine.
			q.Done()
		}()
	}
	wg.Wait()
}

// This tests what happens when there is memory remaining,
// but we would go above the maximum amount of available memory.
func TestController_Error_MaxMemory(t *testing.T) {
	config := config
	config.InitialMemoryBytesQuotaPerQuery = 512
	config.MaxMemoryBytes = 2048
	config.MemoryBytesQuotaPerQuery = 512
	config.QueueSize = 1
	config.ConcurrencyQuota = 1

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory continuously to hit the memory limit.
					for i := 0; i < 16; i++ {
						size := config.MemoryBytesQuotaPerQuery / 16
						if err := alloc.Account(int(size)); err != nil {
							q.SetErr(err)
							return
						}
					}

					// This final allocation should cause an error even though
					// we haven't reached the maximum memory usage for the system.
					if err := alloc.Account(32); err == nil {
						t.Fatal("expected error")
					}
				},
			}, nil
		},
	}

	q, err := ctrl.Query(context.Background(), makeRequest(compiler))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	consumeResults(t, q)
	validateUnusedMemory(t, reg, config)
}

// This tests that we can continuously run queries that do not use
// more than their initial memory allocation with some noisy neighbors.
// The noisy neighbors may occasionally fail because they are competing
// with each other, but they will never cause a small query to fail.
func TestController_NoisyNeighbor(t *testing.T) {
	config := config
	// We are fine using up to 1024 without an additional allocation.
	config.InitialMemoryBytesQuotaPerQuery = 1024
	// Effectively no maximum quota per query.
	config.MemoryBytesQuotaPerQuery = config.InitialMemoryBytesQuotaPerQuery * 100
	// The maximum number is about double what is needed to run
	// all of the queries.
	config.MaxMemoryBytes = config.InitialMemoryBytesQuotaPerQuery * 20
	// The concurrency is 10 which means at most 10 queries can run
	// at any given time.
	config.ConcurrencyQuota = 10
	// Set the queue length to something that can accommodate the input.
	config.QueueSize = 1000

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	wellBehavedNeighbor := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory until we hit our initial memory limit so we should
					// never request more memory.
					for amount := int64(0); amount < config.InitialMemoryBytesQuotaPerQuery; amount += 16 {
						if err := alloc.Account(16); err != nil {
							q.SetErr(fmt.Errorf("well behaved query affected by noisy neighbor: %s", err))
							return
						}
					}
				},
			}, nil
		},
	}

	noisyNeighbor := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory continuously to use up what we can and be as noisy as possible.
					// Turn up the stereo and party on.
					for {
						if err := alloc.Account(16); err != nil {
							// Whoops, party shut down.
							return
						}
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup

	// Launch 100 queriers that are well behaved. They should never fail.
	errCh := make(chan error, 1)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				q, err := ctrl.Query(context.Background(), makeRequest(wellBehavedNeighbor))
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				consumeResults(t, q)
			}
		}()
	}

	// Launch 10 noisy neighbors. They will fail continuously.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				q, err := ctrl.Query(context.Background(), makeRequest(noisyNeighbor))
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				consumeResults(t, q)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error: %s", err)
	}
	validateUnusedMemory(t, reg, config)
}

// This tests that a query that should be allowed is killed
// when it attempts to use more memory available than the
// system has.
func TestController_Error_NoRemainingMemory(t *testing.T) {
	config := config
	// We are fine using up to 1024 without an additional allocation.
	config.InitialMemoryBytesQuotaPerQuery = 1024
	// Effectively no maximum quota per query.
	config.MemoryBytesQuotaPerQuery = config.InitialMemoryBytesQuotaPerQuery * 100
	// The maximum memory available on the system is double the initial quota.
	config.MaxMemoryBytes = config.InitialMemoryBytesQuotaPerQuery * 2

	// Need to limit concurrency along with max memory or the config validation complains
	config.ConcurrencyQuota = 1
	config.QueueSize = 1

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory continuously to use up what we can until denied.
					for size := int64(0); ; size += 16 {
						if err := alloc.Account(16); err != nil {
							// We were not allowed to allocate more.
							// Ensure that the size never exceeded the
							// MaxMemoryBytes value.
							if size > config.MaxMemoryBytes {
								t.Errorf("query was allowed to allocate more than the maximum memory: %d > %d", size, config.MaxMemoryBytes)
							}
							return
						}
					}
				},
			}, nil
		},
	}

	q, err := ctrl.Query(context.Background(), makeRequest(compiler))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	consumeResults(t, q)
	validateUnusedMemory(t, reg, config)
}

// This test ensures the memory that the extra memory allocated
// for a query is properly returned when the query exits.
func TestController_MemoryRelease(t *testing.T) {
	config := config
	config.InitialMemoryBytesQuotaPerQuery = 16
	config.MemoryBytesQuotaPerQuery = 1024
	config.MaxMemoryBytes = config.MemoryBytesQuotaPerQuery * 2
	config.QueueSize = 1
	config.ConcurrencyQuota = 1

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate some amount of memory and never release it.
					if err := alloc.Account(int(config.MemoryBytesQuotaPerQuery) / 2); err != nil {
						q.SetErr(err)
						return
					}
				},
			}, nil
		},
	}

	// Run 100 queries. If we do not release the memory properly,
	// this would fail.
	for i := 0; i < 100; i++ {
		q, err := ctrl.Query(context.Background(), makeRequest(compiler))
		if err != nil {
			t.Errorf("unexpected error: %s", err)
			return
		}
		consumeResults(t, q)

		if t.Failed() {
			return
		}
	}
	validateUnusedMemory(t, reg, config)
}

// Set an irregular memory quota so that doubling the limit continuously
// would send us over the memory quota limit and make sure that
// the quota is still enforced correctly.
func TestController_IrregularMemoryQuota(t *testing.T) {
	config := config
	config.InitialMemoryBytesQuotaPerQuery = 64
	config.MemoryBytesQuotaPerQuery = 768
	config.MaxMemoryBytes = config.MemoryBytesQuotaPerQuery * 2
	config.QueueSize = 1
	config.ConcurrencyQuota = 1

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory continuously to hit the memory limit.
					for size := 0; size < 768; size += 16 {
						if err := alloc.Account(16); err != nil {
							q.SetErr(err)
							return
						}
					}

					// This final allocation should cause an error since we reached the
					// memory quota. If the code for setting the limit is faulty, this
					// would end up being allowed since the limit was set incorrectly.
					if err := alloc.Account(16); err == nil {
						t.Fatal("expected error")
					}
				},
			}, nil
		},
	}

	q, err := ctrl.Query(context.Background(), makeRequest(compiler))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	consumeResults(t, q)
	validateUnusedMemory(t, reg, config)
}

// This tests that if we run a bunch of queries that reserve memory,
// we don't encounter any race conditions that cause the currently
// in use memory from the pool to be accounted incorrectly.
func TestController_ReserveMemoryWithoutExceedingMax(t *testing.T) {
	config := config
	// Small initial memory bytes allocation so most of the query
	// is handled by the memory pool.
	config.InitialMemoryBytesQuotaPerQuery = 16
	// We will allocate 1024. This is needed to ensure the queries do not
	// allocate too much.
	config.MemoryBytesQuotaPerQuery = 1024
	// The maximum amount of memory. We will run with a concurrency of
	// 100 and each of these queries will allocate exactly 1024.
	config.MaxMemoryBytes = 1024 * 100
	// The concurrency is 100 which means at most 100 queries can run
	// at any given time.
	config.ConcurrencyQuota = 100
	// Set the queue length to something that can accommodate the input.
	config.QueueSize = 1000

	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)
	reg := setupPromRegistry(ctrl)

	compiler := &mock.Compiler{
		CompileFn: func(ctx context.Context) (flux.Program, error) {
			return &mock.Program{
				ExecuteFn: func(ctx context.Context, q *mock.Query, alloc *memory.Allocator) {
					// Allocate memory continuously to use up what we can and be as noisy as possible.
					// Turn up the stereo and party on.
					for size := 0; size < 1024; size += 16 {
						if err := alloc.Account(16); err != nil {
							q.SetErr(err)
							return
						}
					}
				},
			}, nil
		},
	}

	var wg sync.WaitGroup

	// Launch double the number of running queriers to ensure saturation.
	errCh := make(chan error, 1)
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				q, err := ctrl.Query(context.Background(), makeRequest(compiler))
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				consumeResults(t, q)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error: %s", err)
	}
	validateUnusedMemory(t, reg, config)
}

func TestController_QueryTracing(t *testing.T) {
	// temporarily install a mock tracer to see which spans are created.
	oldTracer := opentracing.GlobalTracer()
	defer opentracing.SetGlobalTracer(oldTracer)
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	const memoryBytesQuotaPerQuery = 64
	config := config
	config.MemoryBytesQuotaPerQuery = memoryBytesQuotaPerQuery
	ctrl, err := control.New(config, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, ctrl)

	flagger := pmock.NewFlagger(map[feature.Flag]interface{}{
		feature.QueryTracing(): true,
	})
	plainCtx := context.Background()
	withFlagger, err := feature.Annotate(plainCtx, flagger)
	if err != nil {
		t.Fatal(err)
	}
	tcs := []struct {
		name          string
		ctx           context.Context
		doNotWantSpan string
		wantSpan      string
	}{
		{
			name:          "feature flag off",
			ctx:           plainCtx,
			doNotWantSpan: "*executetest.AllocatingFromProcedureSpec",
		},
		{
			name:     "feature flag on",
			ctx:      withFlagger,
			wantSpan: "*executetest.AllocatingFromProcedureSpec",
		},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockTracer.Reset()

			compiler := &mock.Compiler{
				CompileFn: func(ctx context.Context) (flux.Program, error) {
					// Return a program that will allocate one more byte than is allowed.
					pts := plantest.PlanSpec{
						Nodes: []plan.Node{
							plan.CreatePhysicalNode("allocating-from-test", &executetest.AllocatingFromProcedureSpec{
								ByteCount: 16,
							}),
							plan.CreatePhysicalNode("yield", &universe.YieldProcedureSpec{Name: "_result"}),
						},
						Edges: [][2]int{
							{0, 1},
						},
						Resources: flux.ResourceManagement{
							ConcurrencyQuota: 1,
						},
					}

					ps := plantest.CreatePlanSpec(&pts)
					prog := &lang.Program{
						Logger:   zaptest.NewLogger(t),
						PlanSpec: ps,
					}

					return prog, nil
				},
			}

			// Depending on how the feature flag is set in the context,
			// we may or may not do query tracing here.
			q, err := ctrl.Query(tc.ctx, makeRequest(compiler))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			consumeResults(t, q)
			gotSpans := make(map[string]struct{})
			for _, span := range mockTracer.FinishedSpans() {
				gotSpans[span.OperationName] = struct{}{}
			}
			if _, found := gotSpans[tc.doNotWantSpan]; tc.doNotWantSpan != "" && found {
				t.Fatalf("did not want to find span %q but it was there", tc.doNotWantSpan)
			}
			if _, found := gotSpans[tc.wantSpan]; tc.wantSpan != "" && !found {
				t.Fatalf("wanted to find span %q but it was not there", tc.wantSpan)
			}
		})
	}
}

func consumeResults(tb testing.TB, q flux.Query) {
	tb.Helper()
	for res := range q.Results() {
		if err := res.Tables().Do(func(table flux.Table) error {
			return nil
		}); err != nil {
			tb.Errorf("unexpected error: %s", err)
		}
	}
	q.Done()

	if err := q.Err(); err != nil {
		tb.Errorf("unexpected error: %s", err)
	}
}

func shutdown(t *testing.T, ctrl *control.Controller) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ctrl.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func makeRequest(c flux.Compiler) *query.Request {
	return &query.Request{
		Compiler: c,
	}
}
