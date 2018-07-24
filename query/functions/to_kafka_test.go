package functions_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
	kafka "github.com/segmentio/kafka-go"
)

// type kafkaClientMock = func

func TestToKafka_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database",
			Raw:  `from(db:"mydb") |> toKafka(brokers:["brokerurl:8989"], name:"series1", topic:"totallynotfaketopic")`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "toKafka1",
						Spec: &functions.ToKafkaOpSpec{
							Brokers:      []string{"brokerurl:8989"},
							Topic:        "totallynotfaketopic", //Balancer: &kafka.Hash{},
							Name:         "series1",
							TimeColumn:   execute.DefaultTimeColLabel,
							ValueColumns: []string{execute.DefaultValueColLabel},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "toKafka1"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

type kafkaMock struct {
	sync.Mutex
	data [][]kafka.Message
}

func (k *kafkaMock) reset() {
	k.Lock()
	k.data = [][]kafka.Message{}
	k.Unlock()
}

func (k *kafkaMock) Close() error { return nil }

func (k *kafkaMock) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	k.Lock()
	k.data = append(k.data, msgs)
	k.Unlock()
	return nil
}

func TestToKafka_Process(t *testing.T) {
	data := &kafkaMock{}
	functions.DefaultKafkaWriterFactory = func(_ kafka.WriterConfig) functions.KafkaWriter {
		return data
	}

	type wanted struct {
		Table  []*executetest.Table
		Result [][]kafka.Message
	}

	testCases := []struct {
		name string
		spec *functions.ToKafkaProcedureSpec
		data []query.Table
		want wanted
	}{
		{
			name: "coltable with name in _measurement",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Table{execute.CopyTable(&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_measurement", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
					{Label: "fred", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", 2.0, "one"},
					{execute.Time(21), "a", 2.0, "one"},
					{execute.Time(21), "b", 1.0, "seven"},
					{execute.Time(31), "a", 3.0, "nine"},
					{execute.Time(41), "c", 4.0, "elevendyone"},
				},
			}, executetest.UnlimitedAllocator)},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("a _value=2 11"), Key: []byte{0xf1, 0xb0, 0x29, 0xd7, 0x9d, 0x04, 0x31, 0x7c}},
					{Value: []byte("a _value=2 21"), Key: []byte{0xb5, 0xc2, 0xe4, 0x78, 0x95, 0xe0, 0x62, 0x66}},
					{Value: []byte("b _value=1 21"), Key: []byte{0x0e, 0x62, 0x4e, 0xe7, 0x36, 0xac, 0x77, 0xf3}},
					{Value: []byte("a _value=3 31"), Key: []byte{0xf5, 0xd5, 0x22, 0x4d, 0x27, 0x9d, 0x8d, 0xb5}},
					{Value: []byte("c _value=4 41"), Key: []byte{0x05, 0x5b, 0xc5, 0x41, 0x67, 0x78, 0x04, 0xda}},
				}},
			},
		},
		{
			name: "one table with measurement name in _measurement",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_measurement", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
					{Label: "fred", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", 2.0, "one"},
					{execute.Time(21), "a", 2.0, "one"},
					{execute.Time(21), "b", 1.0, "seven"},
					{execute.Time(31), "a", 3.0, "nine"},
					{execute.Time(41), "c", 4.0, "elevendyone"},
				},
			}},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("a _value=2 11"), Key: []byte{0xf1, 0xb0, 0x29, 0xd7, 0x9d, 0x04, 0x31, 0x7c}},
					{Value: []byte("a _value=2 21"), Key: []byte{0xb5, 0xc2, 0xe4, 0x78, 0x95, 0xe0, 0x62, 0x66}},
					{Value: []byte("b _value=1 21"), Key: []byte{0x0e, 0x62, 0x4e, 0xe7, 0x36, 0xac, 0x77, 0xf3}},
					{Value: []byte("a _value=3 31"), Key: []byte{0xf5, 0xd5, 0x22, 0x4d, 0x27, 0x9d, 0x8d, 0xb5}},
					{Value: []byte("c _value=4 41"), Key: []byte{0x05, 0x5b, 0xc5, 0x41, 0x67, 0x78, 0x04, 0xda}},
				}},
			},
		},
		{
			name: "one table with measurement name in _measurement and tag",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_measurement", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
					{Label: "fred", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", 2.0, "one"},
					{execute.Time(21), "a", 2.0, "one"},
					{execute.Time(21), "b", 1.0, "seven"},
					{execute.Time(31), "a", 3.0, "nine"},
					{execute.Time(41), "c", 4.0, "elevendyone"},
				},
			}},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("a,fred=one _value=2 11"), Key: []byte{0xe9, 0xde, 0xc5, 0x1e, 0xfb, 0x26, 0x77, 0xfe}},
					{Value: []byte("a,fred=one _value=2 21"), Key: []byte{0x52, 0x6d, 0x0a, 0xe8, 0x1d, 0xb3, 0xe5, 0xeb}},
					{Value: []byte("b,fred=seven _value=1 21"), Key: []byte{0x18, 0x91, 0xed, 0x7e, 0x79, 0x5c, 0xc2, 0xe3}},
					{Value: []byte("a,fred=nine _value=3 31"), Key: []byte{0x75, 0x15, 0xe5, 0x3e, 0xdd, 0xfd, 0x4f, 0x9a}},
					{Value: []byte("c,fred=elevendyone _value=4 41"), Key: []byte{0xd4, 0xc9, 0xca, 0xea, 0xa6, 0x8d, 0x14, 0x4b}},
				}},
			},
		},
		{
			name: "one table",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					Name:         "one_block",
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), 2.0},
					{execute.Time(21), 1.0},
					{execute.Time(31), 3.0},
					{execute.Time(41), 4.0},
				},
			}},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("one_block _value=2 11"), Key: []byte{0x92, 0x7e, 0x77, 0xb1, 0x2c, 0x35, 0x13, 0x12}},
					{Value: []byte("one_block _value=1 21"), Key: []byte{0x39, 0x39, 0xb2, 0x11, 0xd1, 0x1b, 0x44, 0x57}},
					{Value: []byte("one_block _value=3 31"), Key: []byte{0xa2, 0xc1, 0x71, 0x42, 0xa8, 0xbb, 0x91, 0x67}},
					{Value: []byte("one_block _value=4 41"), Key: []byte{0x82, 0x3b, 0x2e, 0x58, 0xec, 0x53, 0x62, 0x4e}},
				}},
			},
		},
		{
			name: "one table with unused tag",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					Name:         "one_block_w_unused_tag",
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "fred", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), 2.0, "one"},
					{execute.Time(21), 1.0, "seven"},
					{execute.Time(31), 3.0, "nine"},
					{execute.Time(41), 4.0, "elevendyone"},
				},
			}},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("one_block_w_unused_tag _value=2 11"), Key: []byte{0x62, 0xda, 0xe8, 0xc3, 0x2b, 0x88, 0x74, 0x54}},
					{Value: []byte("one_block_w_unused_tag _value=1 21"), Key: []byte{0xff, 0x23, 0xa3, 0x84, 0xe4, 0xcb, 0x77, 0x79}},
					{Value: []byte("one_block_w_unused_tag _value=3 31"), Key: []byte{0xc5, 0x02, 0x43, 0x34, 0x66, 0xb6, 0x43, 0x87}},
					{Value: []byte("one_block_w_unused_tag _value=4 41"), Key: []byte{0x65, 0xd6, 0x94, 0x12, 0xfa, 0x92, 0x30, 0xff}},
				}},
			},
		},
		{
			name: "one table with tag",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "one_block_w_tag",
				},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "fred", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), 2.0, "one"},
					{execute.Time(21), 1.0, "seven"},
					{execute.Time(31), 3.0, "nine"},
					{execute.Time(41), 4.0, "elevendyone"},
				},
			}},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("one_block_w_tag,fred=one _value=2 11"), Key: []byte{0xca, 0xc3, 0xec, 0x04, 0x42, 0xec, 0x85, 0x84}},
					{Value: []byte("one_block_w_tag,fred=seven _value=1 21"), Key: []byte{0x6c, 0x2b, 0xb7, 0xf8, 0x98, 0xce, 0x12, 0x64}},
					{Value: []byte("one_block_w_tag,fred=nine _value=3 31"), Key: []byte{0x41, 0x73, 0x13, 0xd6, 0x5c, 0xf1, 0x18, 0xd3}},
					{Value: []byte("one_block_w_tag,fred=elevendyone _value=4 41"), Key: []byte{0x83, 0x42, 0x25, 0x68, 0x66, 0x44, 0x67, 0x14}},
				}},
			},
		},
		{
			name: "multi table",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "multi_block",
				},
			},
			data: []query.Table{
				&executetest.Table{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "fred", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), 2.0, "one"},
						{execute.Time(21), 1.0, "seven"},
						{execute.Time(31), 3.0, "nine"},
					},
				},
				&executetest.Table{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "fred", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(51), 2.0, "one"},
						{execute.Time(61), 1.0, "seven"},
						{execute.Time(71), 3.0, "nine"},
					},
				},
			},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("multi_block,fred=one _value=2 11"), Key: []byte{0x41, 0x9d, 0x7f, 0x17, 0xc8, 0x21, 0xfb, 0x69}},
					{Value: []byte("multi_block,fred=seven _value=1 21"), Key: []byte{0x8f, 0x83, 0x72, 0x66, 0x7b, 0x78, 0x77, 0x18}},
					{Value: []byte("multi_block,fred=nine _value=3 31"), Key: []byte{0x1c, 0x4a, 0x50, 0x5f, 0xa1, 0xfc, 0xf3, 0x56}},
				}, {
					{Value: []byte("multi_block,fred=one _value=2 51"), Key: []byte{0x77, 0x44, 0x9c, 0x9c, 0x68, 0xca, 0xc1, 0x13}},
					{Value: []byte("multi_block,fred=seven _value=1 61"), Key: []byte{0x48, 0x23, 0xfe, 0x07, 0x61, 0x79, 0x09, 0x74}},
					{Value: []byte("multi_block,fred=nine _value=3 71"), Key: []byte{0x74, 0xc5, 0xa3, 0x42, 0xcb, 0x91, 0x99, 0x7c}},
				}},
			},
		},
		{
			name: "multi collist tables",
			spec: &functions.ToKafkaProcedureSpec{
				Spec: &functions.ToKafkaOpSpec{
					Brokers:      []string{"brokerurl:8989"},
					Topic:        "totallynotfaketopic",
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "multi_collist_blocks",
				},
			},
			data: []query.Table{
				execute.CopyTable(
					&executetest.Table{
						ColMeta: []query.ColMeta{
							{Label: "_time", Type: query.TTime},
							{Label: "_value", Type: query.TFloat},
							{Label: "fred", Type: query.TString},
						},
						Data: [][]interface{}{
							{execute.Time(11), 2.0, "one"},
							{execute.Time(21), 1.0, "seven"},
							{execute.Time(31), 3.0, "nine"},
						},
					}, executetest.UnlimitedAllocator),
				&executetest.Table{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "fred", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(51), 2.0, "one"},
						{execute.Time(61), 1.0, "seven"},
						{execute.Time(71), 3.0, "nine"},
					},
				},
			},
			want: wanted{
				Table: []*executetest.Table(nil),
				Result: [][]kafka.Message{{
					{Value: []byte("multi_collist_blocks,fred=one _value=2 11"), Key: []byte{0xfc, 0xab, 0xa3, 0x68, 0x81, 0x48, 0x7d, 0x8a}},
					{Value: []byte("multi_collist_blocks,fred=seven _value=1 21"), Key: []byte{0x9f, 0xe1, 0x82, 0x97, 0x49, 0x92, 0x56, 0x1a}},
					{Value: []byte("multi_collist_blocks,fred=nine _value=3 31"), Key: []byte{0x73, 0x3c, 0x1a, 0x62, 0xfa, 0x01, 0xcd, 0xa7}},
				}, {
					{Value: []byte("multi_collist_blocks,fred=one _value=2 51"), Key: []byte{0xb9, 0x23, 0xd6, 0x3a, 0x7e, 0x71, 0xa6, 0xde}},
					{Value: []byte("multi_collist_blocks,fred=seven _value=1 61"), Key: []byte{0x0a, 0x70, 0x1f, 0xbe, 0xfd, 0x40, 0x2f, 0xd8}},
					{Value: []byte("multi_collist_blocks,fred=nine _value=3 71"), Key: []byte{0x67, 0x4b, 0xf0, 0xf1, 0xb0, 0xf5, 0x99, 0x5a}},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.Table,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return functions.NewToKafkaTransformation(d, c, tc.spec)
				},
			)
			if !cmp.Equal(tc.want.Result, data.data, cmpopts.EquateNaNs()) {
				t.Log(cmp.Diff(tc.want.Result, data.data))
				t.Fail()
			}
			data.reset()
		})
	}
}
