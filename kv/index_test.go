package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func Test_Inmem_Index(t *testing.T) {
	s, closeStore, err := influxdbtesting.NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore()

	influxdbtesting.TestIndex(t, s)
}

func Test_Bolt_Index(t *testing.T) {
	s, closeBolt, err := influxdbtesting.NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.TestIndex(t, s)
}

func TestIndexKey(t *testing.T) {
	tests := []struct {
		name   string
		fk     string
		pk     string
		expKey string
		expErr error
	}{
		{
			name:   "returns key",
			fk:     "fk_part",
			pk:     "pk_part",
			expKey: "fk_part/pk_part",
		},
		{
			name:   "returns error for invalid foreign key",
			fk:     "fk/part",
			pk:     "pk_part",
			expErr: kv.ErrKeyInvalidCharacters,
		},
		{
			name:   "returns error for invalid primary key",
			fk:     "fk_part",
			pk:     "pk/part",
			expErr: kv.ErrKeyInvalidCharacters,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotKey, gotErr := kv.IndexKey([]byte(test.fk), []byte(test.pk))
			if test.expErr != nil {
				require.Error(t, gotErr)
				assert.EqualError(t, test.expErr, gotErr.Error())
				assert.Nil(t, gotKey)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, test.expKey, string(gotKey))
			}
		})
	}
}

func TestIndex_Walk(t *testing.T) {
	t.Run("only selects exact keys", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		type keyValue struct{ key, val []byte }
		makeIndexKV := func(fk, pk string) keyValue {
			key, err := kv.IndexKey([]byte(fk), []byte(pk))
			if err != nil {
				panic(err)
			}
			return keyValue{
				key: key,
				val: []byte(pk),
			}
		}

		makeKV := func(key, val string) keyValue {
			return keyValue{[]byte(key), []byte(val)}
		}

		var (
			sourceBucket = []byte("source")
			indexBucket  = []byte("index")
			foreignKey   = []byte("jenkins")
			idxkeyvals   = []keyValue{
				makeIndexKV("jenkins-aws", "pk1"),
				makeIndexKV("jenkins-aws", "pk2"),
				makeIndexKV("jenkins-aws", "pk3"),
				makeIndexKV("jenkins", "pk4"),
				makeIndexKV("jenkins", "pk5"),
			}
			srckeyvals = []struct{ key, val []byte }{
				makeKV("pk4", "val4"),
				makeKV("pk5", "val5"),
			}
		)

		mapping := kv.NewIndexMapping(sourceBucket, indexBucket, func(data []byte) ([]byte, error) {
			return nil, nil
		})

		tx := mock.NewMockTx(ctrl)

		src := mock.NewMockBucket(ctrl)
		src.EXPECT().
			GetBatch(srckeyvals[0].key, srckeyvals[1].key).
			Return([][]byte{srckeyvals[0].val, srckeyvals[1].val}, nil)

		tx.EXPECT().
			Bucket(sourceBucket).
			Return(src, nil)

		idx := mock.NewMockBucket(ctrl)
		tx.EXPECT().
			Bucket(indexBucket).
			Return(idx, nil)

		cur := mock.NewMockForwardCursor(ctrl)

		i := 0
		cur.EXPECT().
			Next().
			DoAndReturn(func() ([]byte, []byte) {
				var k, v []byte
				if i < len(idxkeyvals) {
					elem := idxkeyvals[i]
					i++
					k, v = elem.key, elem.val
				}

				return k, v
			}).
			Times(len(idxkeyvals) + 1)
		cur.EXPECT().
			Err().
			Return(nil)
		cur.EXPECT().
			Close().
			Return(nil)
		idx.EXPECT().
			ForwardCursor(foreignKey, gomock.Any()).
			Return(cur, nil)

		ctx := context.Background()
		index := kv.NewIndex(mapping, kv.WithIndexReadPathEnabled)

		j := 0
		err := index.Walk(ctx, tx, foreignKey, func(k, v []byte) (bool, error) {
			require.Less(t, j, len(srckeyvals))
			assert.Equal(t, srckeyvals[j].key, k)
			assert.Equal(t, srckeyvals[j].val, v)
			j++
			return true, nil
		})

		assert.NoError(t, err)
	})
}

func Benchmark_Inmem_Index_Walk(b *testing.B) {
	influxdbtesting.BenchmarkIndexWalk(b, inmem.NewKVStore(), 1000, 200)
}

func Benchmark_Bolt_Index_Walk(b *testing.B) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		b.Fatal(errors.New("unable to open temporary boltdb file"))
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(b), path)
	if err := s.Open(context.Background()); err != nil {
		b.Fatal(err)
	}

	defer func() {
		s.Close()
		os.Remove(path)
	}()

	influxdbtesting.BenchmarkIndexWalk(b, s, 1000, 200)
}
