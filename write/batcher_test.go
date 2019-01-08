package write

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

func TestScanLines(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:  "3 lines produced including their newlines",
			input: "m1,t1=v1 f1=1\nm2,t2=v2 f2=2\nm3,t3=v3 f3=3",
			want:  []string{"m1,t1=v1 f1=1\n", "m2,t2=v2 f2=2\n", "m3,t3=v3 f3=3"},
		},
		{
			name:  "single line without newline",
			input: "m1,t1=v1 f1=1",
			want:  []string{"m1,t1=v1 f1=1"},
		},
		{
			name:  "single line with newline",
			input: "m1,t1=v1 f1=1\n",
			want:  []string{"m1,t1=v1 f1=1\n"},
		},
		{
			name:  "no lines",
			input: "",
			want:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := bufio.NewScanner(strings.NewReader(tt.input))
			scanner.Split(ScanLines)
			got := []string{}
			for scanner.Scan() {
				got = append(got, scanner.Text())
			}
			err := scanner.Err()

			if (err != nil) != tt.wantErr {
				t.Errorf("ScanLines() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("%q. ScanLines() = -got/+want %s", tt.name, cmp.Diff(got, tt.want))
			}
		})
	}
}

// errorReader mocks io.Reader but returns an error.
type errorReader struct{}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("error")
}

func TestBatcher_read(t *testing.T) {
	type args struct {
		cancel bool
		r      io.Reader
		lines  chan []byte
		errC   chan error
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "reading two lines produces 2 lines",
			args: args{
				r:     strings.NewReader("m1,t1=v1 f1=1\nm2,t2=v2 f2=2"),
				lines: make(chan []byte),
				errC:  make(chan error, 1),
			},
			want: []string{"m1,t1=v1 f1=1\n", "m2,t2=v2 f2=2"},
		},
		{
			name: "canceling returns no lines",
			args: args{
				cancel: true,
				r:      strings.NewReader("m1,t1=v1 f1=1"),
				lines:  make(chan []byte),
				errC:   make(chan error, 1),
			},
			want:    []string{},
			wantErr: true,
		},
		{
			name: "error from reader returns error",
			args: args{
				r:     &errorReader{},
				lines: make(chan []byte),
				errC:  make(chan error, 1),
			},
			want:    []string{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.args.cancel {
				ctx, cancel = context.WithCancel(ctx)
			}

			b := &Batcher{}
			got := []string{}

			go b.read(ctx, tt.args.r, tt.args.lines, tt.args.errC)
			if cancel != nil {
				cancel()
			} else {
				for line := range tt.args.lines {
					got = append(got, string(line))
				}
			}

			err := <-tt.args.errC
			if (err != nil) != tt.wantErr {
				t.Errorf("ScanLines.read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("%q. Batcher.read() = -got/+want %s", tt.name, cmp.Diff(got, tt.want))
			}
		})
	}
}

func TestBatcher_write(t *testing.T) {
	type fields struct {
		MaxFlushBytes    int
		MaxFlushInterval time.Duration
	}
	type args struct {
		cancel     bool
		writeError bool
		org        platform.ID
		bucket     platform.ID
		line       string
		lines      chan []byte
		errC       chan error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "sending a single line will send a line to the service",
			fields: fields{
				MaxFlushBytes: 1,
			},
			args: args{
				org:    platform.ID(1),
				bucket: platform.ID(2),
				line:   "m1,t1=v1 f1=1",
				lines:  make(chan []byte),
				errC:   make(chan error),
			},
			want: "m1,t1=v1 f1=1",
		},
		{
			name: "sending a single line and waiting for a timeout will send a line to the service",
			fields: fields{
				MaxFlushInterval: time.Millisecond,
			},
			args: args{
				org:    platform.ID(1),
				bucket: platform.ID(2),
				line:   "m1,t1=v1 f1=1",
				lines:  make(chan []byte),
				errC:   make(chan error),
			},
			want: "m1,t1=v1 f1=1",
		},
		{
			name: "write service returning error stops the write after timeout",
			fields: fields{
				MaxFlushInterval: time.Millisecond,
			},
			args: args{
				writeError: true,
				org:        platform.ID(1),
				bucket:     platform.ID(2),
				line:       "m1,t1=v1 f1=1",
				lines:      make(chan []byte),
				errC:       make(chan error),
			},
			wantErr: true,
		},
		{
			name: "canceling will write no data to service",
			fields: fields{
				MaxFlushBytes: 1,
			},
			args: args{
				cancel: true,
				org:    platform.ID(1),
				bucket: platform.ID(2),
				line:   "m1,t1=v1 f1=1",
				lines:  make(chan []byte),
				errC:   make(chan error),
			},
			wantErr: true,
		},
		{
			name: "write service returning error stops the write",
			fields: fields{
				MaxFlushBytes: 1,
			},
			args: args{
				writeError: true,
				org:        platform.ID(1),
				bucket:     platform.ID(2),
				line:       "m1,t1=v1 f1=1",
				lines:      make(chan []byte),
				errC:       make(chan error),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.args.cancel {
				ctx, cancel = context.WithCancel(ctx)
			}

			// mocking the write service here to either return an error
			// or get back all the bytes from the reader.
			var got string
			svc := &mock.WriteService{
				WriteF: func(ctx context.Context, org, bucket platform.ID, r io.Reader) error {
					if tt.args.writeError {
						return fmt.Errorf("error")
					}
					b, err := ioutil.ReadAll(r)
					got = string(b)
					return err
				},
			}

			b := &Batcher{
				MaxFlushBytes:    tt.fields.MaxFlushBytes,
				MaxFlushInterval: tt.fields.MaxFlushInterval,
				Service:          svc,
			}

			go b.write(ctx, tt.args.org, tt.args.bucket, tt.args.lines, tt.args.errC)

			if cancel != nil {
				cancel()
			} else {
				tt.args.lines <- []byte(tt.args.line)
				// if the max flush interval is not zero,  we are testing to see
				// if the data is flushed via the timer rather than forced by
				// closing the channel.
				if tt.fields.MaxFlushInterval != 0 {
					time.Sleep(tt.fields.MaxFlushInterval * 100)
				}
				close(tt.args.lines)
			}

			err := <-tt.args.errC
			if (err != nil) != tt.wantErr {
				t.Errorf("ScanLines.read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("%q. Batcher.write() = -got/+want %s", tt.name, cmp.Diff(got, tt.want))
			}
		})
	}
}

func TestBatcher_Write(t *testing.T) {
	type fields struct {
		MaxFlushBytes    int
		MaxFlushInterval time.Duration
	}
	type args struct {
		writeError bool
		org        platform.ID
		bucket     platform.ID
		r          io.Reader
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        string
		wantFlushes int
		wantErr     bool
	}{
		{
			name: "a line of line protocol is sent to the service",
			fields: fields{
				MaxFlushBytes: 1,
			},
			args: args{
				org:    platform.ID(1),
				bucket: platform.ID(2),
				r:      strings.NewReader("m1,t1=v1 f1=1"),
			},
			want:        "m1,t1=v1 f1=1",
			wantFlushes: 1,
		},
		{
			name: "multiple lines cause multiple flushes",
			fields: fields{
				MaxFlushBytes: len([]byte("m1,t1=v1 f1=1\n")),
			},
			args: args{
				org:    platform.ID(1),
				bucket: platform.ID(2),
				r:      strings.NewReader("m1,t1=v1 f1=1\nm2,t2=v2 f2=2\nm3,t3=v3 f3=3"),
			},
			want:        "m3,t3=v3 f3=3",
			wantFlushes: 3,
		},
		{
			name:   "errors during read return error",
			fields: fields{},
			args: args{
				org:    platform.ID(1),
				bucket: platform.ID(2),
				r:      &errorReader{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// mocking the write service here to either return an error
			// or get back all the bytes from the reader.
			var (
				got        string
				gotFlushes int
			)
			svc := &mock.WriteService{
				WriteF: func(ctx context.Context, org, bucket platform.ID, r io.Reader) error {
					if tt.args.writeError {
						return fmt.Errorf("error")
					}
					b, err := ioutil.ReadAll(r)
					got = string(b)
					gotFlushes++
					return err
				},
			}

			b := &Batcher{
				MaxFlushBytes:    tt.fields.MaxFlushBytes,
				MaxFlushInterval: tt.fields.MaxFlushInterval,
				Service:          svc,
			}

			ctx := context.Background()
			if err := b.Write(ctx, tt.args.org, tt.args.bucket, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("Batcher.Write() error = %v, wantErr %v", err, tt.wantErr)
			}

			if gotFlushes != tt.wantFlushes {
				t.Errorf("%q. Batcher.Write() flushes %d want %d", tt.name, gotFlushes, tt.wantFlushes)
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("%q. Batcher.Write() = -got/+want %s", tt.name, cmp.Diff(got, tt.want))
			}
		})
	}
}

func TestBatcher_WriteTimeout(t *testing.T) {
	// mocking the write service here to either return an error
	// or get back all the bytes from the reader.
	var got string
	svc := &mock.WriteService{
		WriteF: func(ctx context.Context, org, bucket platform.ID, r io.Reader) error {
			b, err := ioutil.ReadAll(r)
			got = string(b)
			return err
		},
	}

	b := &Batcher{
		Service: svc,
	}

	// this mimics a reader like stdin that may never return data.
	r, _ := io.Pipe()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	if err := b.Write(ctx, platform.ID(1), platform.ID(2), r); err != context.DeadlineExceeded {
		t.Errorf("Batcher.Write() with timeout error = %v", err)
	}

	if got != "" {
		t.Errorf(" Batcher.Write() with timeout got %s", got)
	}
}
