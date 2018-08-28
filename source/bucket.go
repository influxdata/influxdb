package source

import (
	"fmt"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/http/influxdb"
)

// NewBucketService creates a bucket service from a source.
func NewBucketService(s *platform.Source) (platform.BucketService, error) {
	switch s.Type {
	case platform.SelfSourceType:
		// TODO(fntlnz): this is supposed to call a bucket service directly locally,
		// we are letting it err for now since we have some refactoring to do on
		// how services are instantiated
		return nil, fmt.Errorf("self source type not implemented")
	case platform.V2SourceType:
		return &http.BucketService{
			Addr:               s.URL,
			InsecureSkipVerify: s.InsecureSkipVerify,
			Token:              s.Token,
		}, nil
	case platform.V1SourceType:
		return &influxdb.BucketService{
			Source: s,
		}, nil
	}
	return nil, fmt.Errorf("unsupported source type %s", s.Type)
}
