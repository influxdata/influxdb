package secret

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

// Logger is a logger service middleware for secrets
type Logger struct {
	logger        *zap.Logger
	secretService influxdb.SecretService
}

var _ influxdb.SecretService = (*Logger)(nil)

// NewLogger returns a logging service middleware for the User Service.
func NewLogger(log *zap.Logger, s influxdb.SecretService) *Logger {
	return &Logger{
		logger:        log,
		secretService: s,
	}
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (l *Logger) LoadSecret(ctx context.Context, orgID platform.ID, key string) (str string, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to load secret", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret load", dur)
	}(time.Now())
	return l.secretService.LoadSecret(ctx, orgID, key)

}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (l *Logger) GetSecretKeys(ctx context.Context, orgID platform.ID) (strs []string, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to get secret keys", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret get keys", dur)
	}(time.Now())
	return l.secretService.GetSecretKeys(ctx, orgID)

}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (l *Logger) PutSecret(ctx context.Context, orgID platform.ID, key string, val string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to put secret", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret put", dur)
	}(time.Now())
	return l.secretService.PutSecret(ctx, orgID, key, val)

}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (l *Logger) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to put secrets", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret puts", dur)
	}(time.Now())
	return l.secretService.PutSecrets(ctx, orgID, m)

}

// PatchSecrets patches all provided secrets and updates any previous values.
func (l *Logger) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to patch secret", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret patch", dur)
	}(time.Now())
	return l.secretService.PatchSecrets(ctx, orgID, m)

}

// DeleteSecret removes a single secret from the secret store.
func (l *Logger) DeleteSecret(ctx context.Context, orgID platform.ID, keys ...string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete secret", zap.Error(err), dur)
			return
		}
		l.logger.Debug("secret delete", dur)
	}(time.Now())
	return l.secretService.DeleteSecret(ctx, orgID, keys...)

}
