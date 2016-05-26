package influxql_test

import (
	"math"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

func almostEqual(got, exp float64) bool {
	return math.Abs(got-exp) < 1e-5 && !math.IsNaN(got)
}

func TestHoltWinters_AusTourists(t *testing.T) {
	hw := influxql.NewFloatHoltWintersReducer(10, 4, false, 1)
	// Dataset from http://www.inside-r.org/packages/cran/fpp/docs/austourists
	austourists := []influxql.FloatPoint{
		{Time: 1, Value: 30.052513},
		{Time: 2, Value: 19.148496},
		{Time: 3, Value: 25.317692},
		{Time: 4, Value: 27.591437},
		{Time: 5, Value: 32.076456},
		{Time: 6, Value: 23.487961},
		{Time: 7, Value: 28.47594},
		{Time: 8, Value: 35.123753},
		{Time: 9, Value: 36.838485},
		{Time: 10, Value: 25.007017},
		{Time: 11, Value: 30.72223},
		{Time: 12, Value: 28.693759},
		{Time: 13, Value: 36.640986},
		{Time: 14, Value: 23.824609},
		{Time: 15, Value: 29.311683},
		{Time: 16, Value: 31.770309},
		{Time: 17, Value: 35.177877},
		{Time: 18, Value: 19.775244},
		{Time: 19, Value: 29.60175},
		{Time: 20, Value: 34.538842},
		{Time: 21, Value: 41.273599},
		{Time: 22, Value: 26.655862},
		{Time: 23, Value: 28.279859},
		{Time: 24, Value: 35.191153},
		{Time: 25, Value: 41.727458},
		{Time: 26, Value: 24.04185},
		{Time: 27, Value: 32.328103},
		{Time: 28, Value: 37.328708},
		{Time: 29, Value: 46.213153},
		{Time: 30, Value: 29.346326},
		{Time: 31, Value: 36.48291},
		{Time: 32, Value: 42.977719},
		{Time: 33, Value: 48.901525},
		{Time: 34, Value: 31.180221},
		{Time: 35, Value: 37.717881},
		{Time: 36, Value: 40.420211},
		{Time: 37, Value: 51.206863},
		{Time: 38, Value: 31.887228},
		{Time: 39, Value: 40.978263},
		{Time: 40, Value: 43.772491},
		{Time: 41, Value: 55.558567},
		{Time: 42, Value: 33.850915},
		{Time: 43, Value: 42.076383},
		{Time: 44, Value: 45.642292},
		{Time: 45, Value: 59.76678},
		{Time: 46, Value: 35.191877},
		{Time: 47, Value: 44.319737},
		{Time: 48, Value: 47.913736},
	}

	for _, p := range austourists {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: 49, Value: 57.01368875810684},
		{Time: 50, Value: 40.190037686564295},
		{Time: 51, Value: 54.90600903429195},
		{Time: 52, Value: 52.61130714223962},
		{Time: 53, Value: 59.85400578890833},
		{Time: 54, Value: 42.21766711269367},
		{Time: 55, Value: 57.65856066704675},
		{Time: 56, Value: 55.26293832246274},
		{Time: 57, Value: 62.83676840257498},
		{Time: 58, Value: 44.34255373999999},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}

	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}

func TestHoltWinters_AusTourists_Missing(t *testing.T) {
	hw := influxql.NewFloatHoltWintersReducer(10, 4, false, 1)
	// Dataset from http://www.inside-r.org/packages/cran/fpp/docs/austourists
	austourists := []influxql.FloatPoint{
		{Time: 1, Value: 30.052513},
		{Time: 3, Value: 25.317692},
		{Time: 4, Value: 27.591437},
		{Time: 5, Value: 32.076456},
		{Time: 6, Value: 23.487961},
		{Time: 7, Value: 28.47594},
		{Time: 9, Value: 36.838485},
		{Time: 10, Value: 25.007017},
		{Time: 11, Value: 30.72223},
		{Time: 12, Value: 28.693759},
		{Time: 13, Value: 36.640986},
		{Time: 14, Value: 23.824609},
		{Time: 15, Value: 29.311683},
		{Time: 16, Value: 31.770309},
		{Time: 17, Value: 35.177877},
		{Time: 19, Value: 29.60175},
		{Time: 20, Value: 34.538842},
		{Time: 21, Value: 41.273599},
		{Time: 22, Value: 26.655862},
		{Time: 23, Value: 28.279859},
		{Time: 24, Value: 35.191153},
		{Time: 25, Value: 41.727458},
		{Time: 26, Value: 24.04185},
		{Time: 27, Value: 32.328103},
		{Time: 28, Value: 37.328708},
		{Time: 30, Value: 29.346326},
		{Time: 31, Value: 36.48291},
		{Time: 32, Value: 42.977719},
		{Time: 34, Value: 31.180221},
		{Time: 35, Value: 37.717881},
		{Time: 36, Value: 40.420211},
		{Time: 37, Value: 51.206863},
		{Time: 38, Value: 31.887228},
		{Time: 41, Value: 55.558567},
		{Time: 42, Value: 33.850915},
		{Time: 43, Value: 42.076383},
		{Time: 44, Value: 45.642292},
		{Time: 45, Value: 59.76678},
		{Time: 46, Value: 35.191877},
		{Time: 47, Value: 44.319737},
		{Time: 48, Value: 47.913736},
	}

	for _, p := range austourists {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: 49, Value: 54.39825435294697},
		{Time: 50, Value: 41.93726334513928},
		{Time: 51, Value: 54.909838091213345},
		{Time: 52, Value: 57.1641355392107},
		{Time: 53, Value: 57.164128921488114},
		{Time: 54, Value: 44.06955989656805},
		{Time: 55, Value: 57.701724090970124},
		{Time: 56, Value: 60.07064109901599},
		{Time: 57, Value: 60.0706341448159},
		{Time: 58, Value: 46.310272882941966},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}

	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}

func TestHoltWinters_USPopulation(t *testing.T) {
	series := []influxql.FloatPoint{
		{Time: 1, Value: 3.93},
		{Time: 2, Value: 5.31},
		{Time: 3, Value: 7.24},
		{Time: 4, Value: 9.64},
		{Time: 5, Value: 12.90},
		{Time: 6, Value: 17.10},
		{Time: 7, Value: 23.20},
		{Time: 8, Value: 31.40},
		{Time: 9, Value: 39.80},
		{Time: 10, Value: 50.20},
		{Time: 11, Value: 62.90},
		{Time: 12, Value: 76.00},
		{Time: 13, Value: 92.00},
		{Time: 14, Value: 105.70},
		{Time: 15, Value: 122.80},
		{Time: 16, Value: 131.70},
		{Time: 17, Value: 151.30},
		{Time: 18, Value: 179.30},
		{Time: 19, Value: 203.20},
	}
	hw := influxql.NewFloatHoltWintersReducer(10, 0, true, 1)
	for _, p := range series {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: 1, Value: 3.93},
		{Time: 2, Value: 4.666229883011407},
		{Time: 3, Value: 7.582703219729101},
		{Time: 4, Value: 11.38750857910578},
		{Time: 5, Value: 16.061458362866013},
		{Time: 6, Value: 21.60339677828295},
		{Time: 7, Value: 28.028797969563485},
		{Time: 8, Value: 35.36898516917276},
		{Time: 9, Value: 43.67088460608491},
		{Time: 10, Value: 52.99725850596854},
		{Time: 11, Value: 63.42738609386119},
		{Time: 12, Value: 75.05818203585376},
		{Time: 13, Value: 88.00575972021298},
		{Time: 14, Value: 102.40746333932526},
		{Time: 15, Value: 118.42440883475055},
		{Time: 16, Value: 136.24459021744897},
		{Time: 17, Value: 156.08662531118478},
		{Time: 18, Value: 178.20423430441988},
		{Time: 19, Value: 202.89156636951475},
		{Time: 20, Value: 230.48951480987003},
		{Time: 21, Value: 261.3931906116184},
		{Time: 22, Value: 296.0607589238543},
		{Time: 23, Value: 335.0238840597938},
		{Time: 24, Value: 378.900077509081},
		{Time: 25, Value: 428.40730185977736},
		{Time: 26, Value: 484.38125346495343},
		{Time: 27, Value: 547.7958305836579},
		{Time: 28, Value: 619.7873945146928},
		{Time: 29, Value: 701.6835524758332},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}
	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}

func TestHoltWinters_USPopulation_Missing(t *testing.T) {
	series := []influxql.FloatPoint{
		{Time: 1, Value: 3.93},
		{Time: 2, Value: 5.31},
		{Time: 3, Value: 7.24},
		{Time: 4, Value: 9.64},
		{Time: 5, Value: 12.90},
		{Time: 6, Value: 17.10},
		{Time: 7, Value: 23.20},
		{Time: 8, Value: 31.40},
		{Time: 10, Value: 50.20},
		{Time: 11, Value: 62.90},
		{Time: 12, Value: 76.00},
		{Time: 13, Value: 92.00},
		{Time: 15, Value: 122.80},
		{Time: 16, Value: 131.70},
		{Time: 17, Value: 151.30},
		{Time: 19, Value: 203.20},
	}
	hw := influxql.NewFloatHoltWintersReducer(10, 0, true, 1)
	for _, p := range series {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: 1, Value: 3.93},
		{Time: 2, Value: -0.48020223172549237},
		{Time: 3, Value: 4.802781783666482},
		{Time: 4, Value: 9.877595464844614},
		{Time: 5, Value: 15.247582981982251},
		{Time: 6, Value: 21.11408248318643},
		{Time: 7, Value: 27.613033499005994},
		{Time: 8, Value: 34.85894117561601},
		{Time: 9, Value: 42.96187408308528},
		{Time: 10, Value: 52.03593886054132},
		{Time: 11, Value: 62.20424563556273},
		{Time: 12, Value: 73.60229860599725},
		{Time: 13, Value: 86.38069796377347},
		{Time: 14, Value: 100.70760028319269},
		{Time: 15, Value: 116.77117933639244},
		{Time: 16, Value: 134.78222852768778},
		{Time: 17, Value: 154.97699617972333},
		{Time: 18, Value: 177.6203209232622},
		{Time: 19, Value: 203.00912417351864},
		{Time: 20, Value: 231.47631387044993},
		{Time: 21, Value: 263.39515509958034},
		{Time: 22, Value: 299.18416724280326},
		{Time: 23, Value: 339.31261310834543},
		{Time: 24, Value: 384.3066526673811},
		{Time: 25, Value: 434.756242430451},
		{Time: 26, Value: 491.3228711104303},
		{Time: 27, Value: 554.748233097815},
		{Time: 28, Value: 625.8639535249647},
		{Time: 29, Value: 705.6024924601211},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}
	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}
func TestHoltWinters_RoundTime(t *testing.T) {
	maxTime := time.Unix(0, influxql.MaxTime).Round(time.Second).UnixNano()
	data := []influxql.FloatPoint{
		{Time: maxTime - int64(5*time.Second+50*time.Millisecond), Value: 1},
		{Time: maxTime - int64(4*time.Second+103*time.Millisecond), Value: 10},
		{Time: maxTime - int64(3*time.Second+223*time.Millisecond), Value: 2},
		{Time: maxTime - int64(2*time.Second+481*time.Millisecond), Value: 11},
	}
	hw := influxql.NewFloatHoltWintersReducer(2, 2, true, time.Second)
	for _, p := range data {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: maxTime - int64(5*time.Second), Value: 1},
		{Time: maxTime - int64(4*time.Second), Value: 10.499068390422073},
		{Time: maxTime - int64(3*time.Second), Value: 2.002458220927272},
		{Time: maxTime - int64(2*time.Second), Value: 10.499826428426315},
		{Time: maxTime - int64(1*time.Second), Value: 2.898110014107811},
		{Time: maxTime - int64(0*time.Second), Value: 10.499786614238138},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}
	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}

func TestHoltWinters_MaxTime(t *testing.T) {
	data := []influxql.FloatPoint{
		{Time: influxql.MaxTime - 1, Value: 1},
		{Time: influxql.MaxTime, Value: 2},
	}
	hw := influxql.NewFloatHoltWintersReducer(1, 0, true, 1)
	for _, p := range data {
		hw.AggregateFloat(&p)
	}
	points := hw.Emit()

	forecasted := []influxql.FloatPoint{
		{Time: influxql.MaxTime - 1, Value: 1},
		{Time: influxql.MaxTime, Value: 2.0058478778784132},
		{Time: influxql.MaxTime + 1, Value: 3.9399400964478106},
	}

	if exp, got := len(forecasted), len(points); exp != got {
		t.Fatalf("unexpected number of points emitted: got %d exp %d", got, exp)
	}
	for i := range forecasted {
		if exp, got := forecasted[i].Time, points[i].Time; got != exp {
			t.Errorf("unexpected time on points[%d] got %v exp %v", i, got, exp)
		}
		if exp, got := forecasted[i].Value, points[i].Value; !almostEqual(got, exp) {
			t.Errorf("unexpected value on points[%d] got %v exp %v", i, got, exp)
		}
	}
}
