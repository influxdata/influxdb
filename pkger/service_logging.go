package pkger

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

type loggingMW struct {
	logger *zap.Logger
	next   SVC
}

// MWLogging adds logging functionality for the service.
func MWLogging(log *zap.Logger) SVCMiddleware {
	return func(svc SVC) SVC {
		return &loggingMW{
			logger: log,
			next:   svc,
		}
	}
}

var _ SVC = (*loggingMW)(nil)

func (s *loggingMW) InitStack(ctx context.Context, userID influxdb.ID, newStack Stack) (stack Stack, err error) {
	defer func(start time.Time) {
		if err == nil {
			return
		}

		s.logger.Error(
			"failed to init stack",
			zap.Error(err),
			zap.Duration("took", time.Since(start)),
			zap.Stringer("orgID", newStack.OrgID),
			zap.Stringer("userID", userID),
			zap.Strings("urls", newStack.URLs),
		)
	}(time.Now())
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *loggingMW) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (pkg *Pkg, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to create pkg", zap.Error(err), dur)
			return
		}
		s.logger.Info("pkg create", append(s.summaryLogFields(pkg.Summary()), dur)...)
	}(time.Now())
	return s.next.CreatePkg(ctx, setters...)
}

func (s *loggingMW) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (sum Summary, diff Diff, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to dry run pkg",
				zap.String("orgID", orgID.String()),
				zap.String("userID", userID.String()),
				zap.Error(err),
				dur,
			)
			return
		}

		var opt ApplyOpt
		for _, o := range opts {
			o(&opt)
		}

		fields := s.summaryLogFields(sum)
		if opt.StackID != 0 {
			fields = append(fields, zap.Stringer("stackID", opt.StackID))
		}
		fields = append(fields, dur)
		s.logger.Info("pkg dry run successful", fields...)
	}(time.Now())
	return s.next.DryRun(ctx, orgID, userID, pkg, opts...)
}

func (s *loggingMW) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (sum Summary, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to apply pkg",
				zap.String("orgID", orgID.String()),
				zap.String("userID", userID.String()),
				zap.Error(err),
				dur,
			)
			return
		}

		fields := s.summaryLogFields(sum)

		opt := applyOptFromOptFns(opts...)
		if opt.StackID != 0 {
			fields = append(fields, zap.Stringer("stackID", opt.StackID))
		}
		fields = append(fields, dur)
		s.logger.Info("pkg apply successful", fields...)
	}(time.Now())
	return s.next.Apply(ctx, orgID, userID, pkg, opts...)
}

func (s *loggingMW) summaryLogFields(sum Summary) []zap.Field {
	potentialFields := []struct {
		key string
		val int
	}{
		{key: "buckets", val: len(sum.Buckets)},
		{key: "checks", val: len(sum.Checks)},
		{key: "dashboards", val: len(sum.Dashboards)},
		{key: "endpoints", val: len(sum.NotificationEndpoints)},
		{key: "labels", val: len(sum.Labels)},
		{key: "label_mappings", val: len(sum.LabelMappings)},
		{key: "rules", val: len(sum.NotificationRules)},
		{key: "secrets", val: len(sum.MissingSecrets)},
		{key: "tasks", val: len(sum.Tasks)},
		{key: "telegrafs", val: len(sum.TelegrafConfigs)},
		{key: "variables", val: len(sum.Variables)},
	}

	var fields []zap.Field
	for _, f := range potentialFields {
		if f.val > 0 {
			fields = append(fields, zap.Int("num_"+f.key, f.val))
		}
	}

	return fields
}
