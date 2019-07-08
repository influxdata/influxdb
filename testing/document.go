package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/mock"
)

// NewDocumentIntegrationTest will test the documents related funcs.
func NewDocumentIntegrationTest(store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		svc := kv.NewService(store)
		mockTimeGen := new(mock.TimeGenerator)
		if err := svc.Initialize(ctx); err != nil {
			t.Fatalf("failed to initialize service: %v", err)
		}

		svc.TimeGenerator = mockTimeGen

		s, err := svc.CreateDocumentStore(ctx, "testing")
		if err != nil {
			t.Fatalf("failed to create document store: %v", err)
		}

		ss, err := svc.FindDocumentStore(ctx, "testing")
		if err != nil {
			t.Fatalf("failed to find document store: %v", err)
		}

		l1 := &influxdb.Label{Name: "l1"}
		l2 := &influxdb.Label{Name: "l2"}
		mustCreateLabels(ctx, svc, l1, l2)
		lBad := &influxdb.Label{ID: MustIDBase16(oneID), Name: "bad"}

		o1 := &influxdb.Organization{Name: "foo"}
		o2 := &influxdb.Organization{Name: "bar"}
		mustCreateOrgs(ctx, svc, o1, o2)

		u1 := &influxdb.User{Name: "yanky"}
		u2 := &influxdb.User{Name: "doodle"}
		mustCreateUsers(ctx, svc, u1, u2)

		mustMakeUsersOrgOwner(ctx, svc, o1.ID, u1.ID)

		mustMakeUsersOrgMember(ctx, svc, o1.ID, u2.ID)
		mustMakeUsersOrgOwner(ctx, svc, o2.ID, u2.ID)

		// TODO(desa): test tokens and authorizations as well.
		s1 := &influxdb.Session{UserID: u1.ID}
		s2 := &influxdb.Session{UserID: u2.ID}

		var d1 *influxdb.Document
		var d2 *influxdb.Document
		var d3 *influxdb.Document

		t.Run("u1 can create document for o1", func(t *testing.T) {
			d1 = &influxdb.Document{
				Meta: influxdb.DocumentMeta{
					Name:        "i1",
					Type:        "type1",
					Description: "desc1",
				},
				Content: map[string]interface{}{
					"v1": "v1",
				},
			}
			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 0, 0, time.UTC)
			if err := s.CreateDocument(ctx, d1, influxdb.AuthorizedWithOrg(s1, o1.Name), influxdb.WithLabel(l1.ID)); err != nil {
				t.Errorf("failed to create document: %v", err)
			}
		})

		t.Run("u2 cannot create document for o1", func(t *testing.T) {
			d2 = &influxdb.Document{
				Meta: influxdb.DocumentMeta{
					Name: "i2",
				},
				Content: map[string]interface{}{
					"i2": "i2",
				},
			}
			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 1, 0, time.UTC)
			if err := s.CreateDocument(ctx, d2, influxdb.AuthorizedWithOrg(s2, o1.Name), influxdb.WithLabel(l2.ID)); err == nil {
				t.Fatalf("should not have be authorized to create document")
			}

			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 1, 0, time.UTC)
			if err := s.CreateDocument(ctx, d2, influxdb.AuthorizedWithOrg(s2, o2.Name)); err != nil {
				t.Errorf("should have been authorized to create document: %v", err)
			}
		})

		t.Run("u1 cannot create document for o2", func(t *testing.T) {
			d3 = &influxdb.Document{
				Meta: influxdb.DocumentMeta{
					Name: "i2",
				},
				Content: map[string]interface{}{
					"k2": "v2",
				},
			}
			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 2, 0, time.UTC)
			if err := s.CreateDocument(ctx, d3, influxdb.AuthorizedWithOrg(s1, o2.Name)); err == nil {
				t.Errorf("should not have be authorized to create document")
			}
		})

		t.Run("can create unowned document", func(t *testing.T) {
			// TODO(desa): should this be allowed?
			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 2, 0, time.UTC)
			if err := s.CreateDocument(ctx, d3); err != nil {
				t.Fatalf("should have been able to create document: %v", err)
			}
		})

		t.Run("can't create document with unexisted label", func(t *testing.T) {
			d4 := &influxdb.Document{
				Meta: influxdb.DocumentMeta{
					Name: "i4",
				},
				Content: map[string]interface{}{
					"k4": "v4",
				},
			}
			err = s.CreateDocument(ctx, d4, influxdb.WithLabel(lBad.ID))
			ErrorsEqual(t, err, &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  "label not found",
			})
		})

		d1.Meta.CreatedAt = time.Date(2009, 1, 2, 3, 0, 0, 0, time.UTC)
		dl1 := new(influxdb.Document)
		*dl1 = *d1
		dl1.Labels = append([]*influxdb.Label{}, l1)

		d2.Meta.CreatedAt = time.Date(2009, 1, 2, 3, 0, 1, 0, time.UTC)
		dl2 := new(influxdb.Document)
		*dl2 = *d2
		dl2.Labels = append([]*influxdb.Label{}, d2.Labels...)

		d3.Meta.CreatedAt = time.Date(2009, 1, 2, 3, 0, 2, 0, time.UTC)

		t.Run("bare call to find returns all documents", func(t *testing.T) {
			ds, err := ss.FindDocuments(ctx)
			if err != nil {
				t.Fatalf("failed to retrieve documents: %v", err)
			}
			if exp, got := []*influxdb.Document{d1, d2, d3}, ds; !docsMetaEqual(exp, got) {
				t.Errorf("documents are different -got/+want\ndiff %s", docsMetaDiff(exp, got))
			}
		})

		t.Run("u1 can see o1s documents by label", func(t *testing.T) {
			ds, err := ss.FindDocuments(ctx, influxdb.AuthorizedWhere(s1), influxdb.IncludeContent, influxdb.IncludeLabels)

			if err != nil {
				t.Fatalf("failed to retrieve documents: %v", err)
			}

			if exp, got := []*influxdb.Document{dl1}, ds; !docsEqual(got, exp) {
				t.Errorf("documents are different -got/+want\ndiff %s", docsDiff(got, exp))
			}
		})

		t.Run("check not found err", func(t *testing.T) {
			_, err := ss.FindDocuments(ctx, influxdb.WhereID(MustIDBase16(fourID)), influxdb.IncludeContent)
			ErrorsEqual(t, err, &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  influxdb.ErrDocumentNotFound,
			})
		})

		t.Run("u2 can see o1 and o2s documents", func(t *testing.T) {
			ds, err := ss.FindDocuments(ctx, influxdb.AuthorizedWhere(s2), influxdb.IncludeContent, influxdb.IncludeLabels)
			if err != nil {
				t.Fatalf("failed to retrieve documents: %v", err)
			}

			if exp, got := []*influxdb.Document{dl1, dl2}, ds; !docsEqual(exp, got) {
				t.Errorf("documents are different -got/+want\ndiff %s", docsDiff(exp, got))
			}
		})

		t.Run("u2 cannot update document d1", func(t *testing.T) {
			d := &influxdb.Document{
				ID: d1.ID,
				Meta: influxdb.DocumentMeta{
					Name: "updatei1",
				},
				Content: map[string]interface{}{
					"updatev1": "updatev1",
				},
			}
			if err := s.UpdateDocument(ctx, d, influxdb.Authorized(s2)); err == nil {
				t.Errorf("should not have been authorized to update document")
				return
			}
		})

		t.Run("u2 can update document d2", func(t *testing.T) {
			d := &influxdb.Document{
				ID: d2.ID,
				Meta: influxdb.DocumentMeta{
					Name: "updatei2",
				},
				Content: map[string]interface{}{
					"updatev2": "updatev2",
				},
			}
			if err := s.UpdateDocument(ctx, d, influxdb.Authorized(s2)); err != nil {
				t.Errorf("unexpected error updating document: %v", err)
			}
		})

		t.Run("u1 can update document d1", func(t *testing.T) {
			if err := s.DeleteDocuments(ctx, influxdb.AuthorizedWhereID(s1, d1.ID)); err != nil {
				t.Errorf("unexpected error deleteing document: %v", err)
			}
		})

	}
}

func mustCreateOrgs(ctx context.Context, svc *kv.Service, os ...*influxdb.Organization) {
	for _, o := range os {
		if err := svc.CreateOrganization(ctx, o); err != nil {
			panic(err)
		}
	}
}

func mustCreateLabels(ctx context.Context, svc *kv.Service, labels ...*influxdb.Label) {
	for _, l := range labels {
		if err := svc.CreateLabel(ctx, l); err != nil {
			panic(err)
		}
	}
}

func mustCreateUsers(ctx context.Context, svc *kv.Service, us ...*influxdb.User) {
	for _, u := range us {
		if err := svc.CreateUser(ctx, u); err != nil {
			panic(err)
		}
	}
}

func mustMakeUsersOrgOwner(ctx context.Context, svc *kv.Service, oid influxdb.ID, uids ...influxdb.ID) {
	for _, uid := range uids {
		m := &influxdb.UserResourceMapping{
			UserID:       uid,
			UserType:     influxdb.Owner,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   oid,
		}

		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			panic(err)
		}
	}
}

func mustMakeUsersOrgMember(ctx context.Context, svc *kv.Service, oid influxdb.ID, uids ...influxdb.ID) {
	for _, uid := range uids {
		m := &influxdb.UserResourceMapping{
			UserID:       uid,
			UserType:     influxdb.Member,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   oid,
		}

		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			panic(err)
		}
	}
}

func docsEqual(i1, i2 interface{}) bool {
	return cmp.Equal(i1, i2, documentCmpOptions...)
}

func docsMetaEqual(i1, i2 interface{}) bool {
	return cmp.Equal(i1, i2, documentMetaCmpOptions...)
}

func docsDiff(i1, i2 interface{}) string {
	return cmp.Diff(i1, i2, documentCmpOptions...)
}

func docsMetaDiff(i1, i2 interface{}) string {
	return cmp.Diff(i1, i2, documentMetaCmpOptions...)
}

var documentMetaCmpOptions = append(documentCmpOptions, cmpopts.IgnoreFields(influxdb.Document{}, "Content", "Labels"))

var documentCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Document) []*influxdb.Document {
		out := append([]*influxdb.Document(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}
