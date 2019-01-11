package testing

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
)

var keyValueLogCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
}

// A log entry is a comparable data structure that is used for testing
type LogEntry struct {
	Key   []byte
	Value []byte
	Time  time.Time
}

// KeyValueLogFields will include the IDGenerator, and keyValueLogs
type KeyValueLogFields struct {
	LogEntries []LogEntry
}

// KeyValueLog tests all the service functions.
func KeyValueLog(
	init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()),
			t *testing.T)
	}{
		{
			name: "AddLogEntry",
			fn:   AddLogEntry,
		},
		{
			name: "ForEachLogEntry",
			fn:   ForEachLogEntry,
		},
		{
			name: "FirstLogEntry",
			fn:   FirstLogEntry,
		},
		{
			name: "LastLogEntry",
			fn:   LastLogEntry,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// AddLogEntry tests the AddLogEntry for the KeyValueLog contract
func AddLogEntry(
	init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()),
	t *testing.T,
) {
	type args struct {
		key   []byte
		value []byte
		time  time.Time
	}
	type wants struct {
		err        error
		logEntries []LogEntry
	}

	tests := []struct {
		name   string
		fields KeyValueLogFields
		args   args
		wants  wants
	}{
		{
			name:   "Add entry to empty log",
			fields: KeyValueLogFields{},
			args: args{
				key:   []byte("abc"),
				value: []byte("hello"),
				time:  time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("hello"),
						Time:  time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "Add entry to non-empty log",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("hat"),
						Time:  time.Date(2009, time.November, 10, 22, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key:   []byte("abc"),
				value: []byte("hello"),
				time:  time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("hat"),
						Time:  time.Date(2009, time.November, 10, 22, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("hello"),
						Time:  time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.AddLogEntry(ctx, tt.args.key, tt.args.value, tt.args.time)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			logEntries := []LogEntry{}
			opts := platform.FindOptions{}
			err = s.ForEachLogEntry(ctx, tt.args.key, opts, func(v []byte, t time.Time) error {
				logEntries = append(logEntries, LogEntry{
					Key:   tt.args.key,
					Value: v,
					Time:  t,
				})
				return nil
			})
			if err != nil {
				t.Fatalf("failed to retrieve log entries: %v", err)
			}
			if diff := cmp.Diff(logEntries, tt.wants.logEntries, keyValueLogCmpOptions...); diff != "" {
				t.Errorf("logEntries are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// ForEachLogEntry tests the AddLogEntry for the KeyValueLog contract
func ForEachLogEntry(
	init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()),
	t *testing.T,
) {
	type args struct {
		key  []byte
		opts platform.FindOptions
	}
	type wants struct {
		err        error
		logEntries []LogEntry
	}

	tests := []struct {
		name   string
		fields KeyValueLogFields
		args   args
		wants  wants
	}{
		{
			name: "all log entries",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key:  []byte("abc"),
				opts: platform.FindOptions{},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "all log entries descending order",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Descending: true,
				},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "all log entries with offset",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Offset: 2,
				},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "for each log entry with limit",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Limit: 3,
				},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "log entries with offset and limit",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Offset: 2,
					Limit:  2,
				},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "descending log entries with offset and limit",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Offset:     2,
					Limit:      2,
					Descending: true,
				},
			},
			wants: wants{
				logEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "offset exceeds log range",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Offset: 5,
				},
			},
			wants: wants{
				logEntries: []LogEntry{},
			},
		},
		{
			name: "offset exceeds log range descending",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
				opts: platform.FindOptions{
					Offset:     5,
					Descending: true,
				},
			},
			wants: wants{
				logEntries: []LogEntry{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			logEntries := []LogEntry{}
			err := s.ForEachLogEntry(ctx, tt.args.key, tt.args.opts, func(v []byte, t time.Time) error {
				logEntries = append(logEntries, LogEntry{
					Key:   tt.args.key,
					Value: v,
					Time:  t,
				})
				return nil
			})
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(logEntries, tt.wants.logEntries, keyValueLogCmpOptions...); diff != "" {
				t.Errorf("logEntries are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FirstLogEntry tests the FirstLogEntry method for the KeyValueLog contract.
func FirstLogEntry(
	init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()),
	t *testing.T,
) {
	type args struct {
		key []byte
	}
	type wants struct {
		err      error
		logEntry LogEntry
	}

	tests := []struct {
		name   string
		fields KeyValueLogFields
		args   args
		wants  wants
	}{
		{
			name: "get first log entry",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
			},
			wants: wants{
				logEntry: LogEntry{
					Key:   []byte("abc"),
					Value: []byte("1"),
					Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			var err error
			logEntry := LogEntry{Key: tt.args.key}
			logEntry.Value, logEntry.Time, err = s.FirstLogEntry(ctx, tt.args.key)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(logEntry, tt.wants.logEntry, keyValueLogCmpOptions...); diff != "" {
				t.Errorf("logEntries are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// LastLogEntry tests the LastLogEntry method for the KeyValueLog contract.
func LastLogEntry(
	init func(KeyValueLogFields, *testing.T) (platform.KeyValueLog, func()),
	t *testing.T,
) {
	type args struct {
		key []byte
	}
	type wants struct {
		err      error
		logEntry LogEntry
	}

	tests := []struct {
		name   string
		fields KeyValueLogFields
		args   args
		wants  wants
	}{
		{
			name: "get last log entry",
			fields: KeyValueLogFields{
				LogEntries: []LogEntry{
					{
						Key:   []byte("abc"),
						Value: []byte("1"),
						Time:  time.Date(2009, time.November, 10, 1, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("2"),
						Time:  time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
						Time:  time.Date(2009, time.November, 10, 3, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("4"),
						Time:  time.Date(2009, time.November, 10, 4, 0, 0, 0, time.UTC),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("5"),
						Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: []byte("abc"),
			},
			wants: wants{
				logEntry: LogEntry{
					Key:   []byte("abc"),
					Value: []byte("5"),
					Time:  time.Date(2009, time.November, 10, 5, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			var err error
			logEntry := LogEntry{Key: tt.args.key}
			logEntry.Value, logEntry.Time, err = s.LastLogEntry(ctx, tt.args.key)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(logEntry, tt.wants.logEntry, keyValueLogCmpOptions...); diff != "" {
				t.Errorf("logEntries are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
