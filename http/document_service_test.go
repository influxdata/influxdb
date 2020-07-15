package http

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	icontext "github.com/influxdata/influxdb/v2/context"
	httpmock "github.com/influxdata/influxdb/v2/http/mock"
	"github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
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
	// Need this to make resource creation work.
	// We are not testing authorization in the setup.
	ctx = icontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(true, nil))

	store := NewTestInmemStore(t)
	svc := kv.NewService(zaptest.NewLogger(t), store)
	ds, err := svc.CreateDocumentStore(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create document store: %v", err)
	}

	org := &influxdb.Organization{Name: "org"}
	itesting.MustCreateOrgs(ctx, svc, org)
	l1 := &influxdb.Label{Name: "l1", OrgID: org.ID}
	l2 := &influxdb.Label{Name: "l2", OrgID: org.ID}
	l3 := &influxdb.Label{Name: "l3", OrgID: org.ID}
	itesting.MustCreateLabels(ctx, svc, l1, l2, l3)
	doc := &influxdb.Document{
		Content:       "I am a free document",
		Labels:        []*influxdb.Label{l1, l3},
		Organizations: map[influxdb.ID]influxdb.UserType{org.ID: influxdb.Owner},
	}
	if err := ds.CreateDocument(ctx, doc); err != nil {
		panic(err)
	}
	// Organizations are needed only for creation.
	// Need to cleanup for comparison later.
	doc.Organizations = nil
	adoc := &influxdb.Document{
		Content:       "I am another document",
		Labels:        []*influxdb.Label{l1, l2},
		Organizations: map[influxdb.ID]influxdb.UserType{org.ID: influxdb.Owner},
	}
	if err := ds.CreateDocument(ctx, adoc); err != nil {
		panic(err)
	}
	// Organizations are needed only for creation.
	// Need to cleanup for comparison later.
	adoc.Organizations = nil
	backend := NewMockDocumentBackend(t)
	backend.HTTPErrorHandler = http.ErrorHandler(0)
	backend.DocumentService = authorizer.NewDocumentService(svc)
	backend.LabelService = authorizer.NewLabelServiceWithOrg(svc, svc)
	serverFn := func(auth influxdb.Authorizer) *httptest.Server {
		handler := httpmock.NewAuthMiddlewareHandler(NewDocumentHandler(backend), auth)
		return httptest.NewServer(handler)
	}
	clientFn := func(serverUrl string) DocumentService {
		return NewDocumentService(mustNewHTTPClient(t, serverUrl, ""))
	}
	f := fixture{
		Org:             org,
		Labels:          []*influxdb.Label{l1, l2, l3},
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

func (f fixture) addLabelPermission(auth *influxdb.Authorization, action influxdb.Action, lid influxdb.ID) {
	ps := []influxdb.Permission{
		{
			Action: action,
			Resource: influxdb.Resource{
				Type: influxdb.LabelsResourceType,
				ID:   &lid,
			},
		},
	}
	if action == influxdb.WriteAction {
		ps = append(ps, influxdb.Permission{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type: influxdb.LabelsResourceType,
				ID:   &lid,
			},
		})
	}
	auth.Permissions = append(auth.Permissions, ps...)
}

// TestDocumentService tests all the service functions using the document HTTP client.
func TestDocumentService(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "CreateDocument",
			fn:   CreateDocument,
		},
		{
			name: "GetDocument",
			fn:   GetDocument,
		},
		{
			name: "GetDocuments",
			fn:   GetDocuments,
		},
		{
			name: "UpdateDocument",
			fn:   UpdateDocument,
		},
		{
			name: "DeleteDocument",
			fn:   DeleteDocument,
		},

		{
			name: "GetLabels",
			fn:   GetLabels,
		},
		{
			name: "AddLabels",
			fn:   AddLabels,
		},
		{
			name: "DeleteLabel",
			fn:   DeleteLabel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t)
		})
	}
}

func CreateDocument(t *testing.T) {
	t.Run("with content", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		orgID := fx.Org.ID
		d := &influxdb.Document{
			Content: "I am the content",
		}
		if err := client.CreateDocument(context.Background(), namespace, orgID, d); err != nil {
			t.Fatal(err)
		}
		if d.ID <= 1 {
			t.Errorf("invalid document id: %v", d.ID)
		}
		if diff := cmp.Diff(d.Content, "I am the content"); diff != "" {
			t.Errorf("got unexpected content:\n\t%s", diff)
		}
		if len(d.Labels) > 0 {
			t.Errorf("got unexpected labels: %v", d.Labels)
		}
	})

	t.Run("with content and labels", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		orgID := fx.Org.ID
		d := &influxdb.Document{
			Content: "I am the content",
			Labels:  fx.Labels,
		}
		if err := client.CreateDocument(context.Background(), namespace, orgID, d); err != nil {
			t.Fatal(err)
		}
		if d.ID <= 1 {
			t.Errorf("invalid document id: %v", d.ID)
		}
		if diff := cmp.Diff(d.Content, "I am the content"); diff != "" {
			t.Errorf("got unexpected content:\n\t%s", diff)
		}
		if diff := cmp.Diff(d.Labels, fx.Labels); diff != "" {
			t.Errorf("got unexpected labels:\n\t%v", diff)
		}
	})

	t.Run("bad label", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		orgID := fx.Org.ID
		d := &influxdb.Document{
			Content: "I am the content",
			Labels: []*influxdb.Label{
				{
					ID:   influxdb.ID(1),
					Name: "bad",
				},
			},
		}
		if err := client.CreateDocument(context.Background(), namespace, orgID, d); err != nil {
			if !strings.Contains(err.Error(), "label not found") {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		d := &influxdb.Document{
			Content: "I am the content",
		}
		if err := client.CreateDocument(context.Background(), namespace, fx.Org.ID, d); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized - insufficient", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		d := &influxdb.Document{
			Content: "I am the content",
		}
		if err := client.CreateDocument(context.Background(), namespace, fx.Org.ID, d); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})
}

func GetDocument(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		want := fx.Document
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		got, err := client.GetDocument(context.Background(), namespace, want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected document:\n\t%s", diff)
		}
	})

	t.Run("non existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		id := fx.Document.ID + 42
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.GetDocument(context.Background(), namespace, id); err != nil {
			if !strings.Contains(err.Error(), "document not found") {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.GetDocument(context.Background(), namespace, fx.Document.ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})
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
		if err != nil {
			t.Fatal(err)
		}
		if len(docs) > 0 {
			t.Fatalf("no document expected, returned %v instead", docs)
		}
	})
}

func UpdateDocument(t *testing.T) {
	t.Run("update content", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		want := fx.Document
		want.Content = "new content"
		if err := client.UpdateDocument(context.Background(), namespace, want); err != nil {
			t.Fatal(err)
		}
		got, err := client.GetDocument(context.Background(), namespace, want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected document:\n\t%s", diff)
		}
	})

	t.Run("update labels", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		want := fx.Document
		want.Labels = want.Labels[:0]
		if err := client.UpdateDocument(context.Background(), namespace, want); err != nil {
			t.Fatal(err)
		}
		got, err := client.GetDocument(context.Background(), namespace, want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected document:\n\t%s", diff)
		}
	})

	t.Run("update meta", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		want := fx.Document
		want.Meta.Name = "new name"
		if err := client.UpdateDocument(context.Background(), namespace, want); err != nil {
			t.Fatal(err)
		}
		got, err := client.GetDocument(context.Background(), namespace, want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected document:\n\t%s", diff)
		}
	})

	t.Run("unauthorized - wrong org", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			for _, p := range a.Permissions {
				*p.Resource.OrgID++
			}
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		want := fx.AnotherDocument
		want.Content = "new content"
		if err := client.UpdateDocument(context.Background(), namespace, want); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized - insufficient permissions", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		want := fx.Document
		want.Content = "new content"
		if err := client.UpdateDocument(context.Background(), namespace, want); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})
}

func DeleteDocument(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		want := fx.Document
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		pre, err := client.GetDocuments(context.Background(), namespace, fx.Org.ID)
		if err != nil {
			t.Fatal(err)
		}
		l := len(pre)
		if err := client.DeleteDocument(context.Background(), namespace, want.ID); err != nil {
			t.Fatal(err)
		}
		got, err := client.GetDocuments(context.Background(), namespace, fx.Org.ID)
		if err != nil {
			t.Fatal(err)
		}
		lgot := len(got)
		lwant := l - 1
		if diff := cmp.Diff(lgot, lwant); diff != "" {
			t.Errorf("got unexpected length of docs:\n\t%v", diff)
		}
	})

	t.Run("non existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		id := fx.Document.ID + 42
		server := serverFn(fx.auth(influxdb.WriteAction))
		defer server.Close()
		client := clientFn(server.URL)
		if err := client.DeleteDocument(context.Background(), namespace, id); err != nil {
			if !strings.Contains(err.Error(), "document not found") {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
		got, err := client.GetDocuments(context.Background(), namespace, fx.Org.ID)
		if err != nil {
			t.Fatal(err)
		}
		lgot := len(got)
		lwant := 2
		if diff := cmp.Diff(lgot, lwant); diff != "" {
			t.Errorf("got unexpected length of docs:\n\t%v", diff)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		if err := client.DeleteDocument(context.Background(), namespace, fx.Document.ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized - insufficient", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		if err := client.DeleteDocument(context.Background(), namespace, fx.Document.ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})
}

func GetLabels(t *testing.T) {
	t.Run("get labels", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		// GetLabels does not pass through the LabelService. Org permissions are enough.
		server := serverFn(fx.auth(influxdb.ReadAction))
		defer server.Close()
		client := clientFn(server.URL)
		got, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := fx.Document.Labels
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected labels:\n\t%s", diff)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.GetDocumentLabels(context.Background(), namespace, fx.AnotherDocument.ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})
}

func AddLabels(t *testing.T) {
	t.Run("add one", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			// The LabelService uses a "standard auth" mode.
			// That's  why we need to add further permissions and the org ones are not enough.
			fx.addLabelPermission(a, influxdb.WriteAction, fx.Labels[1].ID)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		got, err := client.AddDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Labels[1].ID)
		if err != nil {
			t.Fatal(err)
		}
		want := fx.Labels[1]
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got unexpected labels:\n\t%s", diff)
		}
		gotLs, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		wantLs := fx.Labels
		if diff := cmp.Diff(gotLs, wantLs); diff != "" {
			t.Errorf("got unexpected labels:\n\t%s", diff)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(fx.authKO())
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.AddDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Labels[1].ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("unauthorized - insufficient", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			fx.addLabelPermission(a, influxdb.ReadAction, fx.Labels[1].ID)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.AddDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Labels[1].ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
	})

	t.Run("add same twice", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			// The LabelService uses a "standard auth" mode.
			// That's  why we need to add further permissions and the org ones are not enough.
			fx.addLabelPermission(a, influxdb.WriteAction, fx.Labels[0].ID)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		if _, err := client.AddDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Labels[0].ID); err != nil {
			if !strings.Contains(err.Error(), influxdb.ErrLabelExistsOnResource.Msg) {
				t.Errorf("unexpected error: %v", err.Error())
			}
		} else {
			t.Error("expected error got none")
		}
	})
}

func DeleteLabel(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			// The LabelService uses a "standard auth" mode.
			// That's  why we need to add further permissions and the org ones are not enough.
			fx.addLabelPermission(a, influxdb.WriteAction, fx.Document.Labels[0].ID)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		pre, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		l := len(pre)
		if err := client.DeleteDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Document.Labels[0].ID); err != nil {
			t.Fatal(err)
		}
		got, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		lgot := len(got)
		lwant := l - 1
		if diff := cmp.Diff(lgot, lwant); diff != "" {
			t.Errorf("got unexpected length of docs:\n\t%v", diff)
		}
	})

	t.Run("non existing", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		badLId := fx.Labels[2].ID + 42
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			fx.addLabelPermission(a, influxdb.WriteAction, badLId)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		if err := client.DeleteDocumentLabel(context.Background(), namespace, fx.Document.ID, badLId); err != nil {
			if !strings.Contains(err.Error(), "label not found") {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
		got, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		lgot := len(got)
		lwant := 2
		if diff := cmp.Diff(lgot, lwant); diff != "" {
			t.Errorf("got unexpected length of labels:\n\t%v", diff)
		}
	})

	t.Run("unauthorized - insufficient", func(t *testing.T) {
		serverFn, clientFn, fx := setup(t)
		server := serverFn(func() influxdb.Authorizer {
			a := fx.auth(influxdb.WriteAction)
			fx.addLabelPermission(a, influxdb.ReadAction, fx.Document.Labels[0].ID)
			return a
		}())
		defer server.Close()
		client := clientFn(server.URL)
		if err := client.DeleteDocumentLabel(context.Background(), namespace, fx.Document.ID, fx.Document.Labels[0].ID); err != nil {
			if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
				t.Errorf("unexpected error: %v", err)
			}
		} else {
			t.Error("expected error got none")
		}
		got, err := client.GetDocumentLabels(context.Background(), namespace, fx.Document.ID)
		if err != nil {
			t.Fatal(err)
		}
		lgot := len(got)
		lwant := 2
		if diff := cmp.Diff(lgot, lwant); diff != "" {
			t.Errorf("got unexpected length of labels:\n\t%v", diff)
		}
	})
}
