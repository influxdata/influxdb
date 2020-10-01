package influxql

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
)

type EncodingFormat int

func (f *EncodingFormat) UnmarshalJSON(bytes []byte) error {
	var s string
	if err := json.Unmarshal(bytes, &s); err != nil {
		return err
	}

	*f = EncodingFormatFromMimeType(s)
	return nil
}

func (f EncodingFormat) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.ContentType())
}

const (
	EncodingFormatJSON EncodingFormat = iota
	EncodingFormatCSV
	EncodingFormatMessagePack
	EncodingFormatTable
)

// Returns closed encoding format from the specified mime type.
// The default is JSON if no exact match is found.
func EncodingFormatFromMimeType(s string) EncodingFormat {
	switch s {
	case "application/csv", "text/csv":
		return EncodingFormatCSV
	case "text/plain":
		return EncodingFormatTable
	case "application/x-msgpack":
		return EncodingFormatMessagePack
	case "application/json":
		fallthrough
	default:
		return EncodingFormatJSON
	}
}

func (f EncodingFormat) ContentType() string {
	switch f {
	case EncodingFormatCSV:
		return "text/csv"
	case EncodingFormatTable:
		return "text/plain"
	case EncodingFormatMessagePack:
		return "application/x-msgpack"
	case EncodingFormatJSON:
		fallthrough
	default:
		return "application/json"
	}
}

type QueryRequest struct {
	Authorization  *influxdb.Authorization `json:"authorization,omitempty"`
	OrganizationID influxdb.ID             `json:"organization_id"`
	DB             string                  `json:"db"`
	RP             string                  `json:"rp"`
	Epoch          string                  `json:"epoch"`
	EncodingFormat EncodingFormat          `json:"encoding_format"`
	ContentType    string                  `json:"content_type"` // Content type is the desired response format.
	Chunked        bool                    `json:"chunked"`      // Chunked indicates responses should be chunked using ChunkSize
	ChunkSize      int                     `json:"chunk_size"`   // ChunkSize is the number of points to be encoded per batch. 0 indicates no chunking.
	Query          string                  `json:"query"`        // Query contains the InfluxQL.
	Params         map[string]interface{}  `json:"params,omitempty"`
	Source         string                  `json:"source"` // Source represents the ultimate source of the request.
}

// The HTTP query requests represented the body expected by the QueryHandler
func (r *QueryRequest) Valid() error {
	if !r.OrganizationID.Valid() {
		return &influxdb.Error{
			Msg:  "organization_id is not valid",
			Code: influxdb.EInvalid,
		}
	}
	return r.Authorization.Valid()
}
