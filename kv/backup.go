package kv

import (
	"context"
	"io"
)

func (s *Service) Backup(ctx context.Context, w io.Writer) error {
	return s.kv.Backup(ctx, w)
}
