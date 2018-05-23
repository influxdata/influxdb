package functions_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestToHTTP_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(db:"mydb") |> toHTTP(url: "https://localhost:8081", name:"series1", method:"POST",  timeout: 50s)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "toHTTP1",
						Spec: &functions.ToHTTPOpSpec{
							URL:          "https://localhost:8081",
							Name:         "series1",
							Method:       "POST",
							Timeout:      50 * time.Second,
							TimeColumn:   execute.DefaultTimeColLabel,
							ValueColumns: []string{execute.DefaultValueColLabel},
							Headers: map[string]string{
								"Content-Type": "application/vnd.influx",
								"User-Agent":   "ifqld/dev",
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "toHTTP1"},
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

func TestToHTTPOpSpec_UnmarshalJSON(t *testing.T) {
	type fields struct {
		URL         string
		Method      string
		Headers     map[string]string
		URLParams   map[string]string
		Timeout     time.Duration
		NoKeepAlive bool
	}
	tests := []struct {
		name    string
		fields  fields
		bytes   []byte
		wantErr bool
	}{
		{
			name: "happy path",
			bytes: []byte(`
			{
				"id": "toHTTP",
				"kind": "toHTTP",
				"spec": {
				  "url": "https://localhost:8081",
				  "method" :"POST"
				}
			}`),
			fields: fields{
				URL:    "https://localhost:8081",
				Method: "POST",
			},
		}, {
			name: "bad address",
			bytes: []byte(`
		{
			"id": "toHTTP",
			"kind": "toHTTP",
			"spec": {
			  "url": "https://loc	alhost:8081",
			  "method" :"POST"
			}
		}`),
			fields: fields{
				URL:    "https://localhost:8081",
				Method: "POST",
			},
			wantErr: true,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &functions.ToHTTPOpSpec{
				URL:         tt.fields.URL,
				Method:      tt.fields.Method,
				Headers:     tt.fields.Headers,
				URLParams:   tt.fields.URLParams,
				Timeout:     tt.fields.Timeout,
				NoKeepAlive: tt.fields.NoKeepAlive,
			}
			op := &query.Operation{
				ID:   "toHTTP",
				Spec: o,
			}
			if !tt.wantErr {
				querytest.OperationMarshalingTestHelper(t, tt.bytes, op)
			} else if err := o.UnmarshalJSON(tt.bytes); err == nil {
				t.Errorf("ToHTTPOpSpec.UnmarshalJSON() error = %v, wantErr %v for test %s", err, tt.wantErr, tt.name)
			}
		})
	}
}

func TestToHTTP_Process(t *testing.T) {
	data := []byte{}
	wg := sync.WaitGroup{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		serverData, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Log(err)
			t.FailNow()
		}
		data = append(data, serverData...)
	}))
	type wanted struct {
		Block  []*executetest.Block
		Result []byte
	}
	testCases := []struct {
		name string
		spec *functions.ToHTTPProcedureSpec
		data []query.Block
		want wanted
	}{
		{
			name: "colblock with name in _measurement",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Block{execute.CopyBlock(&executetest.Block{
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
				Block:  []*executetest.Block(nil),
				Result: []byte("a _value=2 11\na _value=2 21\nb _value=1 21\na _value=3 31\nc _value=4 41\n")},
		},
		{
			name: "one block with measurement name in _measurement",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Block{&executetest.Block{
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
				Block:  []*executetest.Block(nil),
				Result: []byte("a _value=2 11\na _value=2 21\nb _value=1 21\na _value=3 31\nc _value=4 41\n")},
		},
		{
			name: "one block with measurement name in _measurement and tag",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					NameColumn:   "_measurement",
				},
			},
			data: []query.Block{&executetest.Block{
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
				Block:  []*executetest.Block(nil),
				Result: []byte("a,fred=one _value=2 11\na,fred=one _value=2 21\nb,fred=seven _value=1 21\na,fred=nine _value=3 31\nc,fred=elevendyone _value=4 41\n")},
		},
		{
			name: "one block",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "POST",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					Name:         "one_block",
				},
			},
			data: []query.Block{&executetest.Block{
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
				Block:  []*executetest.Block(nil),
				Result: []byte("one_block _value=2 11\none_block _value=1 21\none_block _value=3 31\none_block _value=4 41\n"),
			},
		},
		{
			name: "one block with unused tag",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					Name:         "one_block_w_unused_tag",
				},
			},
			data: []query.Block{&executetest.Block{
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
				Block: []*executetest.Block(nil),
				Result: []byte(`one_block_w_unused_tag _value=2 11
one_block_w_unused_tag _value=1 21
one_block_w_unused_tag _value=3 31
one_block_w_unused_tag _value=4 41
`),
			},
		},
		{
			name: "one block with tag",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "one_block_w_tag",
				},
			},
			data: []query.Block{&executetest.Block{
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
				Block: []*executetest.Block(nil),
				Result: []byte(`one_block_w_tag,fred=one _value=2 11
one_block_w_tag,fred=seven _value=1 21
one_block_w_tag,fred=nine _value=3 31
one_block_w_tag,fred=elevendyone _value=4 41
`),
			},
		},
		{
			name: "multi block",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "multi_block",
				},
			},
			data: []query.Block{
				&executetest.Block{
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
				&executetest.Block{
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
				Block: []*executetest.Block(nil),
				Result: []byte("multi_block,fred=one _value=2 11\nmulti_block,fred=seven _value=1 21\nmulti_block,fred=nine _value=3 31\n" +
					"multi_block,fred=one _value=2 51\nmulti_block,fred=seven _value=1 61\nmulti_block,fred=nine _value=3 71\n"),
			},
		},
		{
			name: "multi collist blocks",
			spec: &functions.ToHTTPProcedureSpec{
				Spec: &functions.ToHTTPOpSpec{
					URL:          server.URL,
					Method:       "GET",
					Timeout:      50 * time.Second,
					TimeColumn:   execute.DefaultTimeColLabel,
					ValueColumns: []string{"_value"},
					TagColumns:   []string{"fred"},
					Name:         "multi_collist_blocks",
				},
			},
			data: []query.Block{
				execute.CopyBlock(
					&executetest.Block{
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
				&executetest.Block{
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
				Block: []*executetest.Block(nil),
				Result: []byte("multi_collist_blocks,fred=one _value=2 11\nmulti_collist_blocks,fred=seven _value=1 21\nmulti_collist_blocks,fred=nine _value=3 31\n" +
					"multi_collist_blocks,fred=one _value=2 51\nmulti_collist_blocks,fred=seven _value=1 61\nmulti_collist_blocks,fred=nine _value=3 71\n"),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			wg.Add(len(tc.data))

			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.Block,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewToHTTPTransformation(d, c, tc.spec)
				},
			)
			wg.Wait() // wait till we are done getting the data back
			if string(data) != string(tc.want.Result) {
				t.Logf("expected %s, got %s", tc.want.Result, data)
				t.Fail()
			}
			data = data[:0]
		})
	}
}
