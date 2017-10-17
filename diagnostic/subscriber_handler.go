package diagnostic

import (
	"fmt"

	subscriber "github.com/influxdata/influxdb/services/subscriber/diagnostic"
	"go.uber.org/zap"
)

type SubscriberHandler struct {
	l *zap.Logger
}

func (s *Service) SubscriberHandler() subscriber.Handler {
	if s == nil {
		return nil
	}
	return &SubscriberHandler{l: s.l.With(zap.String("service", "subscriber"))}
}

func (h *SubscriberHandler) Opened() {
	h.l.Info("opened service")
}

func (h *SubscriberHandler) Closed() {
	h.l.Info("closed service")
}

func (h *SubscriberHandler) UpdateSubscriptionError(err error) {
	h.l.Info(fmt.Sprint("error updating subscriptions: ", err))
}

func (h *SubscriberHandler) SubscriptionCreateError(name string, err error) {
	h.l.Info(fmt.Sprintf("Subscription creation failed for '%s' with error: %s", name, err))
}

func (h *SubscriberHandler) AddedSubscription(db, rp string) {
	h.l.Info(fmt.Sprintf("added new subscription for %s %s", db, rp))
}

func (h *SubscriberHandler) DeletedSubscription(db, rp string) {
	h.l.Info(fmt.Sprintf("deleted old subscription for %s %s", db, rp))
}

func (h *SubscriberHandler) SkipInsecureVerify() {
	h.l.Info("WARNING: 'insecure-skip-verify' is true. This will skip all certificate verifications.")
}

func (h *SubscriberHandler) Error(err error) {
	h.l.Info(err.Error())
}
