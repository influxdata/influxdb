package pkger

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
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

func (s *loggingMW) InitStack(ctx context.Context, userID platform.ID, newStack StackCreate) (stack Stack, err error) {
	defer func(start time.Time) {
		if err == nil {
			return
		}

		s.logger.Error(
			"failed to init stack",
			zap.Error(err),
			zap.Stringer("orgID", newStack.OrgID),
			zap.Stringer("userID", userID),
			zap.Strings("urls", newStack.TemplateURLs),
			zap.Duration("took", time.Since(start)),
		)
	}(time.Now())
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *loggingMW) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (_ Stack, err error) {
	defer func(start time.Time) {
		if err == nil {
			return
		}

		s.logger.Error(
			"failed to uninstall stack",
			zap.Error(err),
			zap.Stringer("orgID", identifiers.OrgID),
			zap.Stringer("userID", identifiers.OrgID),
			zap.Stringer("stackID", identifiers.StackID),
			zap.Duration("took", time.Since(start)),
		)
	}(time.Now())
	return s.next.UninstallStack(ctx, identifiers)
}

func (s *loggingMW) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (err error) {
	defer func(start time.Time) {
		if err == nil {
			return
		}

		s.logger.Error(
			"failed to delete stack",
			zap.Error(err),
			zap.Stringer("orgID", identifiers.OrgID),
			zap.Stringer("userID", identifiers.OrgID),
			zap.Stringer("stackID", identifiers.StackID),
			zap.Duration("took", time.Since(start)),
		)
	}(time.Now())
	return s.next.DeleteStack(ctx, identifiers)
}

func (s *loggingMW) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) (stacks []Stack, err error) {
	defer func(start time.Time) {
		if err == nil {
			return
		}

		var stackIDs []string
		for _, id := range f.StackIDs {
			stackIDs = append(stackIDs, id.String())
		}

		s.logger.Error(
			"failed to list stacks",
			zap.Error(err),
			zap.Stringer("orgID", orgID),
			zap.Strings("stackIDs", stackIDs),
			zap.Strings("names", f.Names),
			zap.Duration("took", time.Since(start)),
		)
	}(time.Now())
	return s.next.ListStacks(ctx, orgID, f)
}

func (s *loggingMW) ReadStack(ctx context.Context, id platform.ID) (st Stack, err error) {
	defer func(start time.Time) {
		if err != nil {
			s.logger.Error("failed to read stack",
				zap.Error(err),
				zap.String("id", id.String()),
				zap.Duration("took", time.Since(start)),
			)
			return
		}
	}(time.Now())
	return s.next.ReadStack(ctx, id)
}

func (s *loggingMW) UpdateStack(ctx context.Context, upd StackUpdate) (_ Stack, err error) {
	defer func(start time.Time) {
		if err != nil {
			fields := []zap.Field{
				zap.Error(err),
				zap.String("id", upd.ID.String()),
			}
			if upd.Name != nil {
				fields = append(fields, zap.String("name", *upd.Name))
			}
			if upd.Description != nil {
				fields = append(fields, zap.String("desc", *upd.Description))
			}
			fields = append(fields,
				zap.Strings("urls", upd.TemplateURLs),
				zap.Duration("took", time.Since(start)),
			)

			s.logger.Error("failed to update stack", fields...)
			return
		}
	}(time.Now())
	return s.next.UpdateStack(ctx, upd)
}

func (s *loggingMW) Export(ctx context.Context, opts ...ExportOptFn) (template *Template, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to export template", zap.Error(err), dur)
			return
		}
		s.logger.Info("exported template", append(s.summaryLogFields(template.Summary()), dur)...)
	}(time.Now())
	return s.next.Export(ctx, opts...)
}

func (s *loggingMW) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (impact ImpactSummary, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to dry run template",
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

		fields := s.summaryLogFields(impact.Summary)
		if opt.StackID != 0 {
			fields = append(fields, zap.Stringer("stackID", opt.StackID))
		}
		fields = append(fields, dur)
		s.logger.Info("template dry run successful", fields...)
	}(time.Now())
	return s.next.DryRun(ctx, orgID, userID, opts...)
}

func (s *loggingMW) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (impact ImpactSummary, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			s.logger.Error("failed to apply template",
				zap.String("orgID", orgID.String()),
				zap.String("userID", userID.String()),
				zap.Error(err),
				dur,
			)
			return
		}

		fields := s.summaryLogFields(impact.Summary)

		opt := applyOptFromOptFns(opts...)
		if opt.StackID != 0 {
			fields = append(fields, zap.Stringer("stackID", opt.StackID))
		}
		fields = append(fields, dur)
		s.logger.Info("template apply successful", fields...)
	}(time.Now())
	return s.next.Apply(ctx, orgID, userID, opts...)
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
