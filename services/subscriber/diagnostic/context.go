package diagnostic

type Context interface {
	Opened()
	Closed()
	UpdateSubscriptionError(err error)
	SubscriptionCreateError(name string, err error)
	AddedSubscription(db, rp string)
	DeletedSubscription(db, rp string)
	SkipInsecureVerify()
	Error(err error)
}
