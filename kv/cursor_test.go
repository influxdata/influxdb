package kv_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/kv"
)

func TestStaticCursor_First(t *testing.T) {
	type args struct {
		pairs []kv.Pair
	}
	type wants struct {
		key []byte
		val []byte
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "nil pairs",
			args: args{
				pairs: nil,
			},
			wants: wants{},
		},
		{
			name: "empty pairs",
			args: args{
				pairs: []kv.Pair{},
			},
			wants: wants{},
		},
		{
			name: "unsorted pairs",
			args: args{
				pairs: []kv.Pair{
					{
						Key:   []byte("bcd"),
						Value: []byte("yoyo"),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("oyoy"),
					},
				},
			},
			wants: wants{
				key: []byte("abc"),
				val: []byte("oyoy"),
			},
		},
		{
			name: "sorted pairs",
			args: args{
				pairs: []kv.Pair{
					{
						Key:   []byte("abc"),
						Value: []byte("oyoy"),
					},
					{
						Key:   []byte("bcd"),
						Value: []byte("yoyo"),
					},
				},
			},
			wants: wants{
				key: []byte("abc"),
				val: []byte("oyoy"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := kv.NewStaticCursor(tt.args.pairs)

			key, val := cur.First()

			if want, got := tt.wants.key, key; !bytes.Equal(want, got) {
				t.Errorf("exptected to get key %s got %s", string(want), string(got))
			}

			if want, got := tt.wants.val, val; !bytes.Equal(want, got) {
				t.Errorf("exptected to get value %s got %s", string(want), string(got))
			}
		})
	}
}

func TestStaticCursor_Last(t *testing.T) {
	type args struct {
		pairs []kv.Pair
	}
	type wants struct {
		key []byte
		val []byte
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "nil pairs",
			args: args{
				pairs: nil,
			},
			wants: wants{},
		},
		{
			name: "empty pairs",
			args: args{
				pairs: []kv.Pair{},
			},
			wants: wants{},
		},
		{
			name: "unsorted pairs",
			args: args{
				pairs: []kv.Pair{
					{
						Key:   []byte("bcd"),
						Value: []byte("yoyo"),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("oyoy"),
					},
				},
			},
			wants: wants{
				key: []byte("bcd"),
				val: []byte("yoyo"),
			},
		},
		{
			name: "sorted pairs",
			args: args{
				pairs: []kv.Pair{
					{
						Key:   []byte("abc"),
						Value: []byte("oyoy"),
					},
					{
						Key:   []byte("bcd"),
						Value: []byte("yoyo"),
					},
				},
			},
			wants: wants{
				key: []byte("bcd"),
				val: []byte("yoyo"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := kv.NewStaticCursor(tt.args.pairs)

			key, val := cur.Last()

			if want, got := tt.wants.key, key; !bytes.Equal(want, got) {
				t.Errorf("exptected to get key %s got %s", string(want), string(got))
			}

			if want, got := tt.wants.val, val; !bytes.Equal(want, got) {
				t.Errorf("exptected to get value %s got %s", string(want), string(got))
			}
		})
	}
}

func TestStaticCursor_Seek(t *testing.T) {
	type args struct {
		prefix []byte
		pairs  []kv.Pair
	}
	type wants struct {
		key []byte
		val []byte
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "sorted pairs",
			args: args{
				prefix: []byte("bc"),
				pairs: []kv.Pair{
					{
						Key:   []byte("abc"),
						Value: []byte("oyoy"),
					},
					{
						Key:   []byte("abcd"),
						Value: []byte("oyoy"),
					},
					{
						Key:   []byte("bcd"),
						Value: []byte("yoyo"),
					},
					{
						Key:   []byte("bcde"),
						Value: []byte("yoyo"),
					},
					{
						Key:   []byte("cde"),
						Value: []byte("yyoo"),
					},
				},
			},
			wants: wants{
				key: []byte("bcd"),
				val: []byte("yoyo"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := kv.NewStaticCursor(tt.args.pairs)

			key, val := cur.Seek(tt.args.prefix)

			if want, got := tt.wants.key, key; !bytes.Equal(want, got) {
				t.Errorf("exptected to get key %s got %s", string(want), string(got))
			}

			if want, got := tt.wants.val, val; !bytes.Equal(want, got) {
				t.Errorf("exptected to get value %s got %s", string(want), string(got))
			}
		})
	}
}
