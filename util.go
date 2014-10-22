package influxdb

type serializedSeries struct {
	name    string          `json:"name"`
	columns []string        `json:"columns"`
	points  [][]interface{} `json:"points"`
}

//func SortSerializedSeries(s []*serializedSeries) {
//	sort.Sort(BySerializedSeriesNameAsc{s})
//}

func (s serializedSeries) Series(precision TimePrecision) (*protocol.Series, error) {
	points := make([]*protocol.Point, 0, len(s.GetPoints()))
	if hasDuplicates(s.GetColumns()) {
		return nil, fmt.Errorf("Cannot have duplicate field names")
	}

	for _, point := range s.GetPoints() {
		if len(point) != len(s.GetColumns()) {
			return nil, fmt.Errorf("invalid payload")
		}

		values := make([]*protocol.FieldValue, 0, len(point))
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.GetColumns() {

			value := point[idx]
			if field == "time" {
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_timestamp := int64(f)
					switch precision {
					case SecondPrecision:
						_timestamp *= 1000
						fallthrough
					case MillisecondPrecision:
						_timestamp *= 1000
					}

					timestamp = &_timestamp
					continue
				default:
					return nil, fmt.Errorf("time field must be float but is %T (%v)", value, value)
				}
			}

			if field == "sequence_number" {
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_sequenceNumber := uint64(f)
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case json.Number:
				i, err := v.Int64()
				if err == nil {
					values = append(values, &protocol.FieldValue{Int64Value: &i})
					break
				}
				f, err := v.Float64()
				if err != nil {
					return nil, err
				}
				values = append(values, &protocol.FieldValue{DoubleValue: &f})
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				values = append(values, &protocol.FieldValue{IsNull: &TRUE})
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:         values,
			Timestamp:      timestamp,
			SequenceNumber: sequence,
		})
	}

	fields := removeTimestampFieldDefinition(s.GetColumns())

	series := &protocol.Series{
		Name:   protocol.String(s.GetName()),
		Fields: fields,
		Points: points,
	}
	return series, nil
}
