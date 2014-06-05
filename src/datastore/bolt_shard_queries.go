package datastore

import (
	"cluster"
	"parser"
	"protocol"

	"bytes"
	"encoding/binary"
	"errors"
	"regexp"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"github.com/VividCortex/bolt"
)

func (s *BoltShard) getSeries(db *bolt.DB) []string {
	var series []string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("series"))
		if b == nil {
			return errors.New("datastore: bucket does not exist")
		}

		c := b.Cursor()

		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			strKey := string(key)
			series = append(series, strKey)
		}

		return nil
	})

	return series
}

func getFields(db *bolt.DB, series string) []string {
	var fields []string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("fields"))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for key, _ := c.Seek([]byte(series)); key != nil; key, _ = c.Next() {
			parts := strings.Split(string(key), "\x00")
			if parts[0] != series {
				break
			}

			fields = append(fields, parts[1])
		}

		return nil
	})

	return fields
}

func (s *BoltShard) executeListSeriesQuery(db *bolt.DB, querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	seriesNames := s.getSeries(db)

	for i, _ := range seriesNames {
		a := seriesNames[i]
		if !processor.YieldPoint(&a, nil, nil) {
			return nil
		}
	}

	return nil
}

// getMatches returns a slice of strings that match a regexp from
// a given slice of strings.
func getMatches(r *regexp.Regexp, strings []string) []string {
	var matches []string
	for _, s := range strings {
		if r.MatchString(s) {
			matches = append(matches, s)
		}
	}

	return matches
}

func (s *BoltShard) executeSeriesQuery(db *bolt.DB, querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	seriesNames := s.getSeries(db)

	for series, fields := range querySpec.SelectQuery().GetReferencedColumns() {
		if regex, ok := series.GetCompiledRegex(); ok {
			for _, matchedSeries := range getMatches(regex, seriesNames) {
				if !querySpec.HasReadAccess(matchedSeries) {
					continue
				}

				err, keepGoing := executeQueryForSeries(db, querySpec, matchedSeries, fields, processor)
				if err != nil {
					return err
				}
				if !keepGoing {
					break
				}
			}
		} else {
			err, keepGoing := executeQueryForSeries(db, querySpec, series.Name, fields, processor)
			if err != nil {
				return err
			}
			if !keepGoing {
				break
			}
		}
	}

	return nil
}

func splitKey(key string) ([]string, error) {
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) != 2 || len(parts[1]) <= 16 {
		return nil, errors.New("splitKey(): invalid key")
	}
	series := parts[0]
	timestampSequence := parts[1][:16]
	field := parts[1][16:]

	return append([]string(nil), series, timestampSequence, field), nil
}

func decodeTimestampSequence(str string) (uint64, uint64) {
	r := bytes.NewReader([]byte(str))
	var (
		t uint64
		s uint64
	)
	binary.Read(r, binary.BigEndian, &t)
	binary.Read(r, binary.BigEndian, &s)
	return t, s
}

func executeQueryForSeries(db *bolt.DB, querySpec *parser.QuerySpec, series string, fields []string, processor cluster.QueryProcessor) (error, bool) {
	if len(fields) > 0 && fields[0] == "*" {
		fields = getFields(db, series)
	}

	keepGoing := true

	startTime := uint64(querySpec.GetStartTime().UnixNano() / 1000)
	endTime := uint64(querySpec.GetEndTime().UnixNano() / 1000)

	buf := bytes.NewBuffer(nil)
	protoBuf := proto.NewBuffer(nil)

	binary.Write(buf, binary.BigEndian, &startTime)
	startTimeBytes := buf.Bytes()
	buf.Reset()

	seriesOutgoing := &protocol.Series{Name: protocol.String(series), Fields: fields, Points: make([]*protocol.Point, 0)}

	fieldNameIndex := map[string]int{}
	for index, field := range fields {
		fieldNameIndex[field] = index
	}

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		prevTs, prevSeq := uint64(0), uint64(0)
		var point *protocol.Point

		// Reminder: keys are in the following format
		// <series name>\x00<timestamp><sequence number>\x00<field>
		for cKey, cValue := c.Seek(append([]byte(series+"\x00"), startTimeBytes...)); cKey != nil; cKey, cValue = c.Next() {
			parts, splitErr := splitKey(string(cKey))
			if splitErr != nil {
				log.Error(splitErr)
				continue
			}
			t, s := decodeTimestampSequence(parts[1])
			if t > endTime {
				break
			}

			// check if this key has any fields we want
			if _, present := fieldNameIndex[parts[2]]; !present {
				continue
			}

			if t != prevTs && s != prevSeq {
				point = &protocol.Point{Values: make([]*protocol.FieldValue, len(fields), len(fields))}
				seriesOutgoing.Points = append(seriesOutgoing.Points, point)

				signedTimestamp := int64(t)
				point.Timestamp = &signedTimestamp
				point.SequenceNumber = &s
				prevTs = t
				prevSeq = s
			}

			fv := &protocol.FieldValue{}
			protoBuf.SetBuf(cValue)
			err := protoBuf.Unmarshal(fv)
			if err != nil {
				log.Error(err)
				return err
			}

			point.Values[fieldNameIndex[parts[2]]] = fv
		}

		keepGoing = processor.YieldSeries(seriesOutgoing)
		return nil
	}), keepGoing
}
