package influxdb

import (
	influxerror "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	ErrMeasurementSchemaNameTooShort = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "too short",
	}

	ErrMeasurementSchemaNameTooLong = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "too long",
	}

	ErrMeasurementSchemaNameUnderscore = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "must not begin with _",
	}

	ErrMeasurementSchemaNameQuotes = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "must not contains single or double quotes",
	}

	ErrMeasurementSchemaColumnsMissing = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns missing",
	}

	ErrMeasurementSchemaColumnsMissingTime = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns missing time column with a timestamp semantic",
	}

	ErrMeasurementSchemaColumnsTimeInvalidSemantic = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema contains a time column with an invalid semantic",
	}

	ErrMeasurementSchemaColumnsTimestampSemanticInvalidName = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns contains a timestamp column that is not named time",
	}

	ErrMeasurementSchemaColumnsTimestampSemanticDataType = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns contains a time column with a data type",
	}

	ErrMeasurementSchemaColumnsTagSemanticDataType = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns contains a tag column with a data type",
	}

	ErrMeasurementSchemaColumnsFieldSemanticMissingDataType = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns contains a field column with missing data type",
	}

	ErrMeasurementSchemaColumnsMissingFields = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns requires at least one field type column",
	}

	ErrMeasurementSchemaColumnsDuplicateNames = &influxerror.Error{
		Code: influxerror.EInvalid,
		Msg:  "measurement schema columns contains duplicate column names",
	}
)
