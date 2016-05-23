package ponyExpress

import (
	"log"
	"strconv"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

// reporting.go contains functions to emit tags and points from various parts of ponyExpress
// These points are then written to the ("_%v", sf.TestName) database

// These are the tags that ponyExpress adds to any response points
func (pe *ponyExpress) tags(statementID string) map[string]string {
	tags := map[string]string{
		"number_targets": fmtInt(len(pe.addresses)),
		"precision":      pe.precision,
		"writers":        fmtInt(pe.wconc),
		"readers":        fmtInt(pe.qconc),
		"test_id":        pe.testID,
		"statement_id":   statementID,
		"write_interval": pe.wdelay,
		"query_interval": pe.qdelay,
	}
	return tags
}

// These are the tags that the StoreFront adds to any response points
func (sf *StoreFront) tags() map[string]string {
	tags := map[string]string{
		"precision":  sf.Precision,
		"batch_size": fmtInt(sf.BatchSize),
	}
	return tags
}

// This function makes a *client.Point for reporting on writes
func (pe *ponyExpress) writePoint(retries int, statementID string, statusCode int, responseTime time.Duration, addedTags map[string]string, writeBytes int) *influx.Point {

	tags := sumTags(pe.tags(statementID), addedTags)

	fields := map[string]interface{}{
		"status_code":      statusCode,
		"response_time_ns": responseTime.Nanoseconds(),
		"num_bytes":        writeBytes,
	}

	point, err := influx.NewPoint("write", tags, fields, time.Now())

	if err != nil {
		log.Fatalf("Error creating write results point\n  error: %v\n", err)
	}

	return point
}

// This function makes a *client.Point for reporting on queries
func (pe *ponyExpress) queryPoint(statementID string, body []byte, statusCode int, responseTime time.Duration, addedTags map[string]string) *influx.Point {

	tags := sumTags(pe.tags(statementID), addedTags)

	fields := map[string]interface{}{
		"status_code":      statusCode,
		"num_bytes":        len(body),
		"response_time_ns": responseTime.Nanoseconds(),
	}

	point, err := influx.NewPoint("query", tags, fields, time.Now())

	if err != nil {
		log.Fatalf("Error creating query results point\n  error: %v\n", err)
	}

	return point
}

// Adds two map[string]string together
func sumTags(tags1, tags2 map[string]string) map[string]string {
	tags := make(map[string]string)
	// Add all tags from first map to return map
	for k, v := range tags1 {
		tags[k] = v
	}
	// Add all tags from second map to return map
	for k, v := range tags2 {
		tags[k] = v
	}
	return tags
}

// Turns an int into a string
func fmtInt(i int) string {
	return strconv.FormatInt(int64(i), 10)
}
