package query

import (
	"context"
	"fmt"
	"io"
	"time"
)

// LoggingServiceBridge implements ProxyQueryService and logs the queries while consuming a QueryService interface.
type LoggingServiceBridge struct {
	QueryService QueryService
	QueryLogger  Logger
}

// Query executes and logs the query.
func (s *LoggingServiceBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (n int64, err error) {
	var stats Statistics
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		log := Log{
			OrganizationID: req.Request.OrganizationID,
			ProxyRequest:   req,
			ResponseSize:   n,
			Time:           time.Now(),
			Statistics:     stats,
		}
		if err != nil {
			log.Error = err
		}
		s.QueryLogger.Log(log)
	}()

	results, err := s.QueryService.Query(ctx, &req.Request)
	if err != nil {
		return 0, err
	}
	defer results.Cancel()

	// Check for any reported statistics
	if s, ok := results.(Statisticser); ok {
		stats = s.Statistics()
	}

	encoder := req.Dialect.Encoder()
	n, err = encoder.Encode(w, results)
	if err != nil {
		return n, err
	}
	// The results iterator may have had an error independent of encoding errors.
	return n, results.Err()
}
