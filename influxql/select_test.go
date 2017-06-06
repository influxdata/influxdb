package influxql_test

//func benchmarkSelectDedupe(b *testing.B, seriesN, pointsPerSeries int) {
//	stmt := MustParseSelectStatement(`SELECT sval::string FROM cpu`)
//	stmt.Dedupe = true
//
//	var ic IteratorCreator
//	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
//		if opt.Expr != nil {
//			panic("unexpected expression")
//		}
//
//		p := influxql.FloatPoint{
//			Name: "tags",
//			Aux:  []interface{}{nil},
//		}
//
//		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *influxql.FloatPoint {
//			p.Aux[0] = fmt.Sprintf("server%d", i%seriesN)
//			return &p
//		}}, nil
//	}
//
//	b.ResetTimer()
//	benchmarkSelect(b, stmt, &ic)
//}
//
//func BenchmarkSelect_Dedupe_1K(b *testing.B) { benchmarkSelectDedupe(b, 1000, 100) }
//
//func benchmarkSelectTop(b *testing.B, seriesN, pointsPerSeries int) {
//	stmt := MustParseSelectStatement(`SELECT top(sval, 10) FROM cpu`)
//
//	var ic IteratorCreator
//	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
//		if m.Name != "cpu" {
//			b.Fatalf("unexpected source: %s", m.Name)
//		}
//		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`sval`)) {
//			b.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
//		}
//
//		p := influxql.FloatPoint{
//			Name: "cpu",
//		}
//
//		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *influxql.FloatPoint {
//			p.Value = float64(rand.Int63())
//			p.Time = int64(time.Duration(i) * (10 * time.Second))
//			return &p
//		}}, nil
//	}
//
//	b.ResetTimer()
//	benchmarkSelect(b, stmt, &ic)
//}
//
//func BenchmarkSelect_Top_1K(b *testing.B) { benchmarkSelectTop(b, 1000, 1000) }
