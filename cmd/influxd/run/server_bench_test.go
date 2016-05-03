package run_test

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"
)

func BenchmarkServer_Query_Count_1(b *testing.B) {
	benchmarkServerQueryCount(b, 1)
}

func BenchmarkServer_Query_Count_1K(b *testing.B) {
	benchmarkServerQueryCount(b, 1000)
}

func BenchmarkServer_Query_Count_100K(b *testing.B) {
	benchmarkServerQueryCount(b, 100000)
}

func BenchmarkServer_Query_Count_1M(b *testing.B) {
	benchmarkServerQueryCount(b, 1000000)
}

func benchmarkServerQueryCount(b *testing.B, pointN int) {
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	// Write data into server.
	var buf bytes.Buffer
	for i := 0; i < pointN; i++ {
		fmt.Fprintf(&buf, `cpu value=100 %d`, i+1)
		if i != pointN-1 {
			fmt.Fprint(&buf, "\n")
		}
	}
	s.MustWrite("db0", "rp0", buf.String(), nil)

	// Query simple count from server.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if results, err := s.Query(`SELECT count(value) FROM db0.rp0.cpu`); err != nil {
			b.Fatal(err)
		} else if results != fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",%d]]}]}]}`, pointN) {
			b.Fatalf("unexpected result: %s", results)
		}
	}
}

func BenchmarkServer_ShowSeries_1(b *testing.B) {
	benchmarkServerShowSeries(b, 1)
}

func BenchmarkServer_ShowSeries_1K(b *testing.B) {
	benchmarkServerShowSeries(b, 1000)
}

func BenchmarkServer_ShowSeries_100K(b *testing.B) {
	benchmarkServerShowSeries(b, 100000)
}

func BenchmarkServer_ShowSeries_1M(b *testing.B) {
	benchmarkServerShowSeries(b, 1000000)
}

func benchmarkServerShowSeries(b *testing.B, pointN int) {
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	// Write data into server.
	var buf bytes.Buffer
	for i := 0; i < pointN; i++ {
		fmt.Fprintf(&buf, `cpu,host=server%d value=100 %d`, i, i+1)
		if i != pointN-1 {
			fmt.Fprint(&buf, "\n")
		}
	}
	s.MustWrite("db0", "rp0", buf.String(), nil)

	// Query simple count from server.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.QueryWithParams(`SHOW SERIES`, url.Values{"db": {"db0"}}); err != nil {
			b.Fatal(err)
		}
	}
}
