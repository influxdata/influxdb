package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	icontext "github.com/influxdata/influxdb/v2/context"
	httpmock "github.com/influxdata/influxdb/v2/http/mock"
	"github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const namespace = "templates"

type fixture struct {
	Org             *influxdb.Organization
	Labels          []*influxdb.Label
	Document        *influxdb.Document
	AnotherDocument *influxdb.Document
}

func setup(t *testing.T) (func(auth influxdb.Authorizer) *httptest.Server, func(serverUrl string) DocumentService, fixture) {
	ctx := context.Background()

	store := NewTestInmemStore(t)
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)
	svc := kv.NewService(zaptest.NewLogger(t), store, tenantService)

	ds, err := svc.CreateDocumentStore(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create document store: %v", err)
	}

	user := &influxdb.User{Name: "user"}
	itesting.MustCreateUsers(ctx, tenantService, user)

	// Need this to make resource creation work.
	// We are not testing authorization in the setup.
	auth := mock.NewMockAuthorizer(true, nil)
	auth.UserID = user.ID
	ctx = icontext.SetAuthorizer(ctx, auth)

	org := &influxdb.Organization{Name: "org"}
	itesting.MustCreateOrgs(ctx, tenantService, org)
	doc := &influxdb.Document{
		Content: "I am a free document",
	}
	if err := ds.CreateDocument(ctx, doc); err != nil {
		panic(err)
	}
	// Organizations are needed only for creation.
	// Need to cleanup for comparison later.
	doc.Organizations = nil
	adoc := &influxdb.Document{
		Content: "I am another document",
	}
	if err := ds.CreateDocument(ctx, adoc); err != nil {
		panic(err)
	}

	backend := NewMockDocumentBackend(t)
	backend.HTTPErrorHandler = http.ErrorHandler(0)
	backend.DocumentService = authorizer.NewDocumentService(svc)
	serverFn := func(auth influxdb.Authorizer) *httptest.Server {
		handler := httpmock.NewAuthMiddlewareHandler(NewDocumentHandler(backend), auth)
		return httptest.NewServer(handler)
	}
	clientFn := func(serverUrl string) DocumentService {
		return NewDocumentService(mustNewHTTPClient(t, serverUrl, ""))
	}
	f := fixture{
		Org:             org,
		Document:        doc,
		AnotherDocument: adoc,
	}
	return serverFn, clientFn, f
}

func (f fixture) auth(action influxdb.Action) *influxdb.Authorization {
	a := &influxdb.Authorization{
		Permissions: []influxdb.Permission{
			{
				Action: action,
				Resource: influxdb.Resource{
					Type:  influxdb.DocumentsResourceType,
					OrgID: &f.Org.ID,
				},
			},
		},
		Status: influxdb.Active,
	}
	if action == influxdb.WriteAction {
		a.Permissions = append(a.Permissions, influxdb.Permission{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type:  influxdb.DocumentsResourceType,
				OrgID: &f.Org.ID,
			},
		})
	}
	return a
}

func (f fixture) authKO() *influxdb.Authorization {
	return &influxdb.Authorization{
		Status: influxdb.Active,
	}
}

// TestDocumentService tests all the service functions using the document HTTP client.
func TestDocumentService(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "GetDocuments",
			fn:   GetDocuments,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t)
		})
	}
}

func GetDocuments(t *testing.T) {
	t.Run("get existing documents", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		got, err := client.GetDocuments(context.Background(), namespace, fx.Org.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := []*influxdb.Document{fx.Document, fx.AnotherDocument}
		want[0].Content = nil // response will not contain the content of documents
		want[1].Content = nil // response will not contain the content of documents
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected document:\n\t%s", diff)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		docs, err := client.GetDocuments(context.Background(), namespace, fx.Org.ID)
		require.Error(t, err, "call should be unauthorized")
		assert.Empty(t, docs)
	})
}
