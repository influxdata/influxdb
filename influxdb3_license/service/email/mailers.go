package email

import (
	"context"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/mailgun/mailgun-go/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Mailgun implements the Mailer interface using Mailgun's service for delivery
type Mailgun struct {
	mg     *mailgun.MailgunImpl
	domain string
	apiKey string
}

// NewMailgun returns a new instances of a Mailgun
func NewMailgun(domain string, apiKey string) *Mailgun {
	return &Mailgun{
		mg:     mailgun.NewMailgun(domain, apiKey),
		domain: domain,
		apiKey: apiKey,
	}
}

// SendMail implements the SendMail method of the Mailer interface
func (m Mailgun) SendMail(ctx context.Context, email *store.Email) (resp string, id string, err error) {
	msg := mailgun.NewMessage(email.From, email.Subject, email.Body, email.To)

	if email.TemplateName != "" {
		msg.SetTemplate(email.TemplateName)
		if err := msg.AddVariable("verificationLink", email.VerificationURL); err != nil {
			return "", "", err
		}
	}

	resp, id, err = m.mg.Send(ctx, msg)
	return
}

// DebugMailer implements the Mailer interface and logs instead of actually
// sending emails. This can be useful for testing and troubleshooting without
// spamming an external email service and someone's inbox.
type DebugMailer struct {
	SendMailFn func(ctx context.Context, email *store.Email) (resp string, id string, err error)
	logger     *zap.Logger
}

// NewDebugMailer returns a new instance of a DebugMailer
func NewDebugMailer(logger *zap.Logger) *DebugMailer {
	mm := DebugMailer{logger: logger}
	mm.SendMailFn = mm.logSendMail
	return &mm
}

// SendMail implements the SendMail method of the Mailer interface.
// The behavior of this method can be modified by clients by setting the
// SnedMailFn member of the DebugMailer struct.
func (m *DebugMailer) SendMail(ctx context.Context, email *store.Email) (resp string, id string, err error) {
	return m.SendMailFn(ctx, email)
}

// logSendmail is the default function for SendMailFn
func (m *DebugMailer) logSendMail(ctx context.Context, email *store.Email) (resp string, id string, err error) {
	store.LogEmail("DebugMailer SendMail", email, zapcore.InfoLevel, m.logger)
	return "Queued. Thank you", uuid.NewString(), nil
}
