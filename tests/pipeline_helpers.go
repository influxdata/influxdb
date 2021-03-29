package tests

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"go.uber.org/zap"
)

// A Pipeline is responsible for configuring launcher.TestLauncher
// with default values so it may be used for end-to-end integration
// tests.
type Pipeline struct {
	Launcher *launcher.TestLauncher

	DefaultOrgID    platform.ID
	DefaultBucketID platform.ID
	DefaultUserID   platform.ID
}

// NewDefaultPipeline creates a Pipeline with default
// values.
//
// It is retained for compatibility with cloud tests.
func NewDefaultPipeline(t *testing.T, opts ...launcher.OptSetter) *DefaultPipeline {
	setDefaultLogLevel := func(o *launcher.InfluxdOpts) {
		// This is left here mainly for retro compatibility
		if VeryVerbose {
			o.LogLevel = zap.DebugLevel
		} else {
			o.LogLevel = zap.InfoLevel
		}

	}
	// Set the default log level as the FIRST option here so users can override
	// it with passed-in setters.
	opts = append([]launcher.OptSetter{setDefaultLogLevel}, opts...)
	return &DefaultPipeline{Pipeline: NewPipeline(t, opts...)}
}

// NewPipeline returns a pipeline with the given options applied to the configuration as appropriate.
//
// A single user, org, bucket and token are created.
func NewPipeline(tb testing.TB, opts ...launcher.OptSetter) *Pipeline {
	tb.Helper()

	tl := launcher.NewTestLauncher()
	p := &Pipeline{
		Launcher: tl,
	}

	tl.RunOrFail(tb, context.Background(), opts...)

	// setup default operator
	res := p.Launcher.OnBoardOrFail(tb, &influxdb.OnboardingRequest{
		User:                   DefaultUsername,
		Password:               DefaultPassword,
		Org:                    DefaultOrgName,
		Bucket:                 DefaultBucketName,
		RetentionPeriodSeconds: influxdb.InfiniteRetention,
		Token:                  OperToken,
	})

	p.DefaultOrgID = res.Org.ID
	p.DefaultUserID = res.User.ID
	p.DefaultBucketID = res.Bucket.ID

	return p
}

// Open opens all the components of the pipeline.
func (p *Pipeline) Open() error {
	return nil
}

// MustOpen opens the pipeline, panicking if any error is encountered.
func (p *Pipeline) MustOpen() {
	if err := p.Open(); err != nil {
		panic(err)
	}
}

// Close closes all the components of the pipeline.
func (p *Pipeline) Close() error {
	return p.Launcher.Shutdown(context.Background())
}

// MustClose closes the pipeline, panicking if any error is encountered.
func (p *Pipeline) MustClose() {
	if err := p.Close(); err != nil {
		panic(err)
	}
}

// MustNewAdminClient returns a default client that will direct requests to Launcher.
//
// The operator token is authorized to do anything in the system.
func (p *Pipeline) MustNewAdminClient() *Client {
	return p.MustNewClient(p.DefaultOrgID, p.DefaultBucketID, OperToken)
}

// MustNewClient returns a client that will direct requests to Launcher.
func (p *Pipeline) MustNewClient(org, bucket platform.ID, token string) *Client {
	config := ClientConfig{
		UserID:             p.DefaultUserID,
		OrgID:              org,
		BucketID:           bucket,
		DocumentsNamespace: DefaultDocumentsNamespace,
		Token:              token,
	}
	svc, err := NewClient(p.Launcher.URL(), config)
	if err != nil {
		panic(err)
	}
	return svc
}

// NewBrowserClient returns a client with a cookie session that will direct requests to Launcher.
func (p *Pipeline) NewBrowserClient(org, bucket platform.ID, session *influxdb.Session) (*Client, error) {
	config := ClientConfig{
		UserID:             p.DefaultUserID,
		OrgID:              org,
		BucketID:           bucket,
		DocumentsNamespace: DefaultDocumentsNamespace,
		Session:            session,
	}
	return NewClient(p.Launcher.URL(), config)
}

// BrowserFor will create a user, session, and browser client.
// The generated browser points to the given org and bucket.
//
// The user and session are inserted directly into the backing store.
func (p *Pipeline) BrowserFor(org, bucket platform.ID, username string) (*Client, platform.ID, error) {
	ctx := context.Background()
	user := &influxdb.User{
		Name: username,
	}

	err := p.Launcher.UserService().CreateUser(ctx, user)
	if err != nil {
		return nil, 0, err
	}

	session, err := p.Launcher.SessionService().CreateSession(ctx, username)
	if err != nil {
		return nil, 0, err
	}
	client, err := p.NewBrowserClient(org, bucket, session)
	return client, user.ID, err
}

// Flush is a no-op and retained for compatibility with tests from cloud.
func (p *Pipeline) Flush() {
}

// DefaultPipeline is a wrapper for Pipeline and is retained
// for compatibility with cloud tests.
type DefaultPipeline struct {
	*Pipeline
}
