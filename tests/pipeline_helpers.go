package tests

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// A Pipeline is a collection of node types that act as a pipeline for writing
// and reading data.
type Pipeline struct {
	Launcher *launcher.TestLauncher

	DefaultOrgID    influxdb.ID
	DefaultBucketID influxdb.ID
	DefaultUserID   influxdb.ID

	openClosers OpenClosers
}

// pipelineConfig tracks the pre-configuration for a pipeline.
// Because this struct is relatively complex, you should not instantiate it directly,
// but rather pass PipelineOption values to NewPipeline.
type pipelineConfig struct {
	logger *zap.Logger
}

// NewDefaultPipeline creates a Pipeline with one storage node, one query node
// and one gateway node.
//
// Further, a single user, org, bucket and token are created. See
// NewDefaultGatewayNode for more details.
func NewDefaultPipeline(t *testing.T, opts ...PipelineOption) *DefaultPipeline {
	opts = append(WithDefaults(), opts...)
	return &DefaultPipeline{Pipeline: NewPipeline(t, opts...)}
}

// NewPipeline returns a pipeline with the given options applied to the configuration as appropriate.
// Each query node will be connected to the read service of all storage nodes.
//
// Each storage and query node share the same configs.
//
// A single user, org, bucket and token are created. See NewDefaultGatewayNode
// for more details.
func NewPipeline(tb testing.TB, opts ...PipelineOption) *Pipeline {
	tb.Helper()

	var conf pipelineConfig
	for _, o := range opts {
		o.applyConfig(&conf)
	}

	logger := conf.logger
	if logger == nil {
		// This is left here mainly for retro compatibility
		var logLevel zaptest.LoggerOption
		if VeryVerbose {
			logLevel = zaptest.Level(zap.DebugLevel)
		} else {
			logLevel = zaptest.Level(zap.InfoLevel)
		}

		logger = zaptest.NewLogger(tb, logLevel).With(zap.String("test_name", tb.Name()))
	}

	launcherOptions := []launcher.Option{
		launcher.WithLogger(logger),
	}
	for _, o := range opts {
		if opt := o.makeLauncherOption(); opt != nil {
			launcherOptions = append(launcherOptions, opt)
		}
	}

	tl := launcher.NewTestLauncher(nil, launcherOptions...)
	p := &Pipeline{
		Launcher: tl,
	}

	err := tl.Run(context.Background())
	require.NoError(tb, err)

	// setup default operator
	res := p.Launcher.OnBoardOrFail(tb, &influxdb.OnboardingRequest{
		User:            DefaultUsername,
		Password:        DefaultPassword,
		Org:             DefaultOrgName,
		Bucket:          DefaultBucketName,
		RetentionPeriod: 0, // infinite retention period
		Token:           OperToken,
	})

	p.DefaultOrgID = res.Org.ID
	p.DefaultUserID = res.User.ID
	p.DefaultBucketID = res.Bucket.ID

	return p
}

// Open opens all the components of the pipeline.
func (p *Pipeline) Open() error {
	if err := p.openClosers.OpenAll(); err != nil {
		return err
	}

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
	_ = p.Launcher.Shutdown(context.Background())
	return p.openClosers.CloseAll()
}

// MustClose closes the pipeline, panicking if any error is encountered.
func (p *Pipeline) MustClose() {
	if err := p.Close(); err != nil {
		panic(err)
	}
}

// MustNewAdminClient returns a default client pointed at the gateway.
//
// operator token is authorized to do anything in the system.
func (p *Pipeline) MustNewAdminClient() *Client {
	return p.MustNewClient(p.DefaultOrgID, p.DefaultBucketID, OperToken)
}

// MustNewClient returns a client pointed at the gateway.
func (p *Pipeline) MustNewClient(org, bucket influxdb.ID, token string) *Client {
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

// NewBrowserClient returns a client pointed at the gateway with a cookie session.
func (p *Pipeline) NewBrowserClient(org, bucket influxdb.ID, session *influxdb.Session) (*Client, error) {
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
// The user and session are inserted directly into the gateway backing store.
func (p *Pipeline) BrowserFor(org, bucket influxdb.ID, username string) (*Client, influxdb.ID, error) {
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

// Flush makes sure the PointsWriter has flushed all of the writes to kafka and the
// storage nodes have observed the writes.
func (p *Pipeline) Flush() {

}

// DefaultPipeline simplifies some of Pipeline method calls.
type DefaultPipeline struct {
	*Pipeline
	Writer *Client
}
