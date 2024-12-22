package email

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/logger"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

var (
	DomainDefault                     string     = "mailgun.influxdata.com"
	TemplateNameDefault               string     = "influxdb-pro-trial-license"
	MaxEmailsPerUserPerLicenseDefault int        = 3
	RateLimitDefault                  rate.Limit = rate.Every(1 * time.Second)
	RateLimitBurstDefault             int        = 100
	UserRateLimitDefault              rate.Limit = rate.Every(10 * time.Minute)
	UserRateLimitBurstDefault         int        = 10
	IPRateLimitDefault                rate.Limit = rate.Every(10 * time.Minute)
	IPRateLimitBurstDefault           int        = 10

	// MaxQueueLengthDefault is the default value for the maximum number of
	// emails that should be in the 'scheduled' state in the emails table in
	// the database. Rather than pick an arbitrary length, it's better to base
	// it on the maximum tolerable wait time for a user to receive their
	// email address verification email, which means it should be a function
	// of the overall rate limit. Here we're defaulting it to 3 minutes.
	MaxQueueLengthDefault int = int(RateLimitDefault * 3 * 60)

	ErrOverallRateLimitExceeded = errors.New("overall rate limit exceeded")
	ErrUserRateLimitExceeded    = errors.New("user rate limit exceeded")
	ErrIPRateLimitExceeded      = errors.New("ip rate limit exceeded")
	ErrOutboundEmailQueueFull   = errors.New("outbound email queue full")
)

// Mailer is an interface for sending emails that this service uses.
// Any type that implements this interface can be used by this service to send
// or simulate sending emails.
type Mailer interface {
	SendMail(ctx context.Context, email *store.Email) (resp string, id string, err error)
}

// Config holds the configuration settings for the service
type Config struct {
	Domain                     string
	EmailTemplateName          string
	MaxEmailsPerUserPerLicense int
	RateLimit                  rate.Limit
	RateLimitBurst             int
	UserRateLimit              rate.Limit
	UserRateLimitBurst         int
	IPRateLimit                rate.Limit
	IPRateLimitBurst           int
	MaxQueueLength             int
	Mailer                     Mailer
	Store                      store.Store
	Logger                     *zap.Logger
}

// DefaultConfig returns an instance of Config with default values set
func DefaultConfig(mailer Mailer, store store.Store, logger *zap.Logger) *Config {
	return &Config{
		Domain:                     DomainDefault,
		EmailTemplateName:          TemplateNameDefault,
		MaxEmailsPerUserPerLicense: MaxEmailsPerUserPerLicenseDefault,
		RateLimit:                  RateLimitDefault,
		RateLimitBurst:             RateLimitBurstDefault,
		UserRateLimit:              UserRateLimitDefault,
		UserRateLimitBurst:         UserRateLimitBurstDefault,
		IPRateLimit:                IPRateLimitDefault,
		IPRateLimitBurst:           IPRateLimitBurstDefault,
		MaxQueueLength:             MaxQueueLengthDefault,
		Mailer:                     mailer,
		Store:                      store,
		Logger:                     logger,
	}
}

// Service is an email service that implements a database-backed outbound
// email queue with a background worker that pulls emails from the queue
// and uses a Mailer to send them. The service supports per user and
// per IP address rate limits as well as an overall rate limit. The goal
// of the per user and IP rate limiters is to prevent a bad actor from
// using all the overall rate limit.
type Service struct {
	cfg        *Config
	mailer     Mailer
	store      store.Store
	logger     *zap.Logger
	logLimiter *rate.Sometimes
	limiter    *rate.Limiter

	mu           sync.RWMutex
	userLimiters map[int64]*rateLimiterPair
	ipLimiters   map[string]*rateLimiterPair
	wg           *sync.WaitGroup
	doneCh       chan struct{}
	ticker       *time.Ticker
}

// rateLimiterPair pairs a rate limiter with a logging rate limiter.
// Each active user and IP address will have one of these pairs associated
// with it. The limiter will limit the user or IP address request frequency
// and when a user or IP starts getting rate limited, the `sometimes` will
// make sure we log it the first few times and then every so often.
type rateLimiterPair struct {
	limiter   *rate.Limiter
	sometimes *rate.Sometimes
}

// newLogSometimes returns an instance of a rate.Sometimes used by this service
// to ensure that we log enough to identify spammers but not so much that
// would flood the log. Logging a short burst in the beginning will make
// it stand out visually in the logs and show us when an attack started. Then,
// logging periodically will show us that it's either still ongoing or when
// it stopped.
func newLogSometimes() *rate.Sometimes {
	return &rate.Sometimes{First: 10, Interval: time.Minute}
}

// NewService creates a new instance of Service. The Start method
// must be called to start the background queue processor.
func NewService(cfg *Config) *Service {
	return &Service{
		cfg:          cfg,
		mailer:       cfg.Mailer,
		store:        cfg.Store,
		logger:       cfg.Logger,
		logLimiter:   newLogSometimes(),
		limiter:      rate.NewLimiter(cfg.RateLimit, cfg.RateLimitBurst),
		userLimiters: make(map[int64]*rateLimiterPair),
		ipLimiters:   make(map[string]*rateLimiterPair),
	}
}

// Start runs the background email queue processor
func (s *Service) Start(pollInterval time.Duration) {
	log, logEnd := logger.NewOperation(s.logger, "Email service starting", "email_service")
	defer logEnd()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doneCh == nil {
		s.wg = new(sync.WaitGroup)
		s.doneCh = make(chan struct{})
		s.ticker = time.NewTicker(pollInterval)

		s.wg.Add(1)
		go s.runBackground()

		log.Info("Email service started")
	} else {
		log.Info("Email service already started")
	}
}

// Stop halts the background email queue processor
func (s *Service) Stop() {
	log, logEnd := logger.NewOperation(s.logger, "Email service stopping", "email_service")
	defer logEnd()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doneCh != nil {
		close(s.doneCh)
		s.wg.Wait()

		s.doneCh = nil
		s.ticker.Stop()
		s.ticker = nil
		log.Info("Email service stopped")
	} else {
		log.Info("Email service already stopped")
	}
}

// QueueEmail adds an email to the outbound queue (i.e., emails table in the
// database) in a 'scheduled' state for the background processor to send.
//
// Possible return values:
//
//	ErrUserRateLimitExceeded - the user's ID has caused too many email
//	    requests and this request has been ignored. Further requests
//	    from this user may be ignored and the user will be blocked
//	    if this continues.
//	ErrIPRateLimitExceeded - this IP address has caused too many email
//	    requests and this request has been ignored. Further requests
//	    from this IP address may be ignored and the IP address may be
//	    blocked if this continues.
//	ErrOverallRateLimitExceeded - the service's overall rate limit has been
//	    exceeded but the email has been added to the emails table to send.
//	    But, email delivery may take a bit longer than normal.
//	ErrOutboundEmailQueueFull - email not added to the outbound queue
//	    because the overall rate limit has been exceeded long enough to
//	    overflow the queue.
//	Other errors possible - email not added to queue
func (s *Service) QueueEmail(email *store.Email) error {
	log, logEnd := logger.NewOperation(s.logger, "QueueEmail", "email_service")
	defer logEnd()

	ctx := context.Background()
	tx, err := s.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func(tx store.Tx) {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("failed to rollback transaction", zap.Error(err))
		}
	}(tx)

	// Check rate limits before adding the email to the emails table in the
	// database to avoid filling the database with spam.
	limiter, rlimErr := s.checkRateLimits(email)
	switch rlimErr {
	case nil:
		// Nothing to see here. Move along.
	case ErrUserRateLimitExceeded:
		limiter.sometimes.Do(func() {
			store.LogEmail("QueueEmail: user rate limited", email, zapcore.WarnLevel, log)
		})
		return ErrUserRateLimitExceeded
	case ErrIPRateLimitExceeded:
		limiter.sometimes.Do(func() {
			store.LogEmail("QueueEmail: ip rate limited", email, zapcore.WarnLevel, log)
		})
		return ErrIPRateLimitExceeded
	case ErrOverallRateLimitExceeded:
		qlen, err := s.store.GetScheduledEmailCnt(ctx, tx)
		if err != nil {
			log.Error("QueueEmail: error getting scheduled email count", zap.Error(err))
			return err
		}

		if qlen > int64(s.cfg.MaxQueueLength) {
			limiter.sometimes.Do(func() {
				log.Error("QueueEmail: max email queue length exceeded")
			})
			return ErrOutboundEmailQueueFull
		}

		limiter.sometimes.Do(func() {
			log.Warn("QueueEmail: rate limited - queue len increasing", zap.Int64("queue-len", qlen))
		})

		// Intentionally continuing - not returning error here
	default:
		return rlimErr
	}

	// Add the email to the database
	if err := s.store.CreateEmail(ctx, tx, email); err != nil {
		log.Error("QueueEmail: failed to add email to queue", zap.Error(err))
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Error("QueueEmail: failed to commit transaction", zap.Error(err))
		return err
	}

	log.Info("QueueEmail: emailed queued",
		zap.String("to_email", email.ToEmail),
		zap.String("ip_address", email.UserIP.String()),
		zap.String("verification_token", email.VerificationToken))

	if rlimErr != nil {
		return rlimErr
	}

	return nil
}

// runBackground implements the background email queue processor
func (s *Service) runBackground() {
	_, logEnd := logger.NewOperation(s.logger, "Email service running", "email_service")
	defer logEnd()
	defer s.wg.Done()
	for {
		select {
		case <-s.ticker.C:
			s.processEmailQueue()
			s.pruneRateLimiters()
		case <-s.doneCh:
			return
		}
	}
}

// processEmailQueue pulls a batch of emails from the queue (database) and
// attempts to send them using the Mailer.
func (s *Service) processEmailQueue() {
	log, logEnd := logger.NewOperation(s.logger, "processEmailQueue", "email_service")
	defer logEnd()

	// Start a DB transaction
	ctx := context.Background()
	tx, err := s.store.BeginTx(ctx)
	if err != nil {
		log.Error("processEmailQueue: failed to start transaction", zap.Error(err))
		return
	}
	log.Debug("processEmailQueue: store transaction started")

	defer func(tx store.Tx) {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("processEmailQueue: failed to rollback transaction", zap.Error(err))
		}
	}(tx)

	// Get a batch of emails from the queue
	batchSz := 1
	emails, err := s.store.GetScheduledEmailBatch(ctx, tx, batchSz)
	if err != nil {
		log.Error("processEmailQueue: failed to retrieve scheduled emails", zap.Error(err))
		return
	}
	log.Debug("processEmailQueue: processing emails", zap.Int("batch-sz", len(emails)))

	for _, email := range emails {
		if err := s.sendMail(email, ctx, tx, log); err != nil {
			store.LogEmail("processEmailQueue: failed to send email", email, zapcore.ErrorLevel, log)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Error("processEmailQueue: failed to commit transaction", zap.Error(err))
	}
	log.Debug("processEmailQueue: batch sent successfully", zap.Int("batch-sz", len(emails)))
}

// sendMail uses the Mailer to send a single email and update the database
func (s *Service) sendMail(email *store.Email, ctx context.Context, tx store.Tx, log *zap.Logger) error {
	//log, logEnd := logger.NewOperation(s.logger, "sendMail: email service sending email", "email_service")
	//defer logEnd()

	// Check if we've already sent this email to the user the max allowed
	// times. This is to guard against accidentally spamming a user with
	// the same email too many times but does allow it to be sent a few
	// times just in case it got stuck in spam or the user accidentally
	// deleted it the first time, etc. The calling code should filter
	// these out but this is an extra safety measure.
	if email.SendCnt >= s.cfg.MaxEmailsPerUserPerLicense {
		// This send would (further) exceed the max. Log it and return.
		log.Info("sendEmail: skipping send to avoid spamming user",
			zap.String("email", email.ToEmail),
			zap.String("verification_token", email.VerificationToken),
			zap.Int64("license_id", email.LicenseID),
			zap.Int("times_sent", email.SendCnt),
			zap.Int("send_limit", s.cfg.MaxEmailsPerUserPerLicense),
			zap.Int("send_fails", email.SendFailCnt),
		)
	}

	// Send the email
	resp, id, err := s.mailer.SendMail(ctx, email)
	if err != nil {
		store.LogEmail("sendMail: failed to send email", email, zapcore.WarnLevel, log)
		return err
	}

	email.SentAt = time.Now()
	email.SendCnt++
	email.DeliverySrvcResp = resp
	email.DeliverSrvcID = id

	store.LogEmail("sendMail: email sent", email, zapcore.InfoLevel, log)

	// Mark email as sent in DB
	if err := s.store.MarkEmailAsSent(ctx, tx, email); err != nil {
		store.LogEmail("sendMail: failed to mark email sent", email, zapcore.ErrorLevel, log)
		return err
	}

	store.LogEmail("sendMail: email marked sent (commit pending)", email, zapcore.DebugLevel, log)

	return nil
}

// checkRateLimits checks the rate limits for the user ID and IP
// address that triggered this request to send an email. It also checks the
// overall email rate limit.
func (s *Service) checkRateLimits(email *store.Email) (*rateLimiterPair, error) {
	var (
		userIP  = email.UserIP.String()
		userLim *rateLimiterPair
		userOK  bool
		ipLim   *rateLimiterPair
		ipOK    bool
	)

	// The rate limiters are thread-safe but the maps are not so take locks.
	func() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		userLim, userOK = s.userLimiters[email.UserID]
		ipLim, ipOK = s.ipLimiters[userIP]
	}()

	if !userOK || !ipOK {
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !userOK {
				userLim = &rateLimiterPair{
					limiter:   rate.NewLimiter(s.cfg.UserRateLimit, s.cfg.UserRateLimitBurst),
					sometimes: newLogSometimes(),
				}
				s.userLimiters[email.UserID] = userLim
			}
			if !ipOK {
				ipLim = &rateLimiterPair{
					limiter:   rate.NewLimiter(s.cfg.UserRateLimit, s.cfg.UserRateLimitBurst),
					sometimes: newLogSometimes(),
				}
				s.ipLimiters[userIP] = ipLim
			}
		}()
	}

	if !userLim.limiter.Allow() {
		// User is being rate limited.
		return userLim, ErrUserRateLimitExceeded
	}

	// Check the rate limit for the IP address associated with this email.
	if !ipLim.limiter.Allow() {
		// IP address is being rate limited.
		return ipLim, ErrIPRateLimitExceeded
	}

	// Check the overall global rate limiter. We checked user and IP rate
	// limiters first so bad actors don't penalize everyone else.
	if !s.limiter.Allow() {
		return &rateLimiterPair{s.limiter, s.logLimiter}, ErrOverallRateLimitExceeded
	}

	return nil, nil
}

// pruneRateLimiters deletes user and IP based rate limiters if their token
// buckets are full.
func (s *Service) pruneRateLimiters() {
	numUserLimitersBefore := len(s.userLimiters)

	maps.DeleteFunc(s.userLimiters, func(userID int64, pair *rateLimiterPair) bool {
		return int(pair.limiter.Tokens()) >= pair.limiter.Burst()
	})

	numUserLimitersAfter := len(s.userLimiters)

	if numUserLimitersAfter < numUserLimitersBefore {
		s.logger.Info("pruned user rate limiters",
			zap.Int("user_rate_limiters_after_prune", numUserLimitersAfter),
			zap.Int("user_rate_limiters_pruned", numUserLimitersBefore-numUserLimitersAfter),
		)
	}

	numIPLimitersBefore := len(s.ipLimiters)

	maps.DeleteFunc(s.ipLimiters, func(ip string, pair *rateLimiterPair) bool {
		return int(pair.limiter.Tokens()) >= pair.limiter.Burst()
	})

	numIPLimitersAfter := len(s.ipLimiters)

	if numIPLimitersAfter < numIPLimitersBefore {
		s.logger.Info("pruned ip rate limiters",
			zap.Int("num_ip_limiters_after_prune", numIPLimitersAfter),
			zap.Int("num_ip_limiters_before_prune", numIPLimitersBefore))
	}
}
