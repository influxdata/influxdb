package diagnostic

import (
	"io"

	"github.com/uber-go/zap"
)

var nullLogger = zap.New(zap.NullEncoder())

type Service struct {
	l zap.Logger
}

func New(w io.Writer) *Service {
	logger := zap.New(zap.NewTextEncoder(), zap.Output(zap.AddSync(w)))
	return &Service{l: logger}
}

func (s *Service) Logger() zap.Logger {
	if s == nil {
		return nullLogger
	}
	return s.l
}
