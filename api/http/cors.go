package http

import (
	libhttp "net/http"
	stat "statistic"
	"reflect"
	"time"
)

func HeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		begin := time.Now()
		rw.Header().Add("Access-Control-Allow-Origin", "*")
		rw.Header().Add("Access-Control-Max-Age", "2592000")
		rw.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		rw.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		rw.Header().Add("X-Influxdb-Version", version)

		handler(rw, req)
		end := time.Now().Sub(begin)

		stat.Prove(
			stat.NewMetricCustom(stat.TYPE_HTTP_STATUS, func(s *stat.InternalStatistic, metric stat.Metric) {
					key := getStatusCode(rw)
					if _, ok := s.HttpStatus[key]; !ok {
						s.HttpStatus[key] = 0
					}
					s.HttpStatus[key]++
				}),
			stat.NewMetricFloat64(stat.TYPE_HTTPAPI_RESPONSE_TIME, stat.OPERATION_APPEND, end.Seconds()),
		)
	}
}

func CompressionHeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return HeaderHandler(CompressionHandler(true, handler), version)
}

func getStatusCode(w libhttp.ResponseWriter) uint64 {
	var status int64 = 0

	field := reflect.ValueOf(w).Elem().FieldByName("status")
	if field.Kind() == reflect.Int {
		status = field.Int()
	}

	return uint64(status)
}
