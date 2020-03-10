package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/mock"
	"go.uber.org/zap/zaptest"
)

// NewDocumentIntegrationTest will test the documents related funcs.
func NewDocumentIntegrationTest(store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		kvsvc := kv.NewService(zaptest.NewLogger(t), store)
		mockTimeGen := new(mock.TimeGenerator)
		if err := kvsvc.Initialize(ctx); err != nil {
			t.Fatalf("failed to initialize service: %v", err)
		}

		kvsvc.TimeGenerator = mockTimeGen
		svc := authorizer.NewDocumentService(kvsvc)

		s, err := svc.CreateDocumentStore(ctx, "testing")
		if err != nil {
			t.Fatalf("failed to create document store: %v", err)
		}

		ss, err := svc.FindDocumentStore(ctx, "testing")
		if err != nil {
			t.Fatalf("failed to find document store: %v", err)
		}

		l1 := &influxdb.Label{Name: "l1", OrgID: MustIDBase16("41a9f7288d4e2d64")}
		l2 := &influxdb.Label{Name: "l2", OrgID: MustIDBase16("41a9f7288d4e2d64")}
		MustCreateLabels(ctx, kvsvc, l1, l2)
		lBad := &influxdb.Label{ID: MustIDBase16(oneID), Name: "bad"}

		o1 := &influxdb.Organization{Name: "foo"}
		o2 := &influxdb.Organization{Name: "bar"}
		MustCreateOrgs(ctx, kvsvc, o1, o2)

		u1 := &influxdb.User{Name: "yanky"}
		u2 := &influxdb.User{Name: "doodle"}
		MustCreateUsers(ctx, kvsvc, u1, u2)

		MustMakeUsersOrgOwner(ctx, kvsvc, o1.ID, u1.ID)
		MustMakeUsersOrgOwner(ctx, kvsvc, o2.ID, u2.ID)
		MustMakeUsersOrgMember(ctx, kvsvc, o1.ID, u2.ID)

		// TODO(desa): test tokens and authorizations as well.
		now := time.Now()
		s1 := &influxdb.Session{
			CreatedAt: now,
			ExpiresAt: now.Add(1 * time.Hour),
			UserID:    u1.ID,
			Permissions: []influxdb.Permission{
				// u1 is owner of o1
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						OrgID: &o1.ID,
						Type:  influxdb.DocumentsResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						OrgID: &o1.ID,
						Type:  influxdb.DocumentsResourceType,
					},
				},
			},
		}
		s2 := &influxdb.Session{
			CreatedAt: now,
			ExpiresAt: now.Add(1 * time.Hour),
			UserID:    u2.ID,
			Permissions: []influxdb.Permission{
				// u2 is owner of o2
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						OrgID: &o2.ID,
						Type:  influxdb.DocumentsResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						OrgID: &o2.ID,
						Type:  influxdb.DocumentsResourceType,
					},
				},
				// u2 is member of o1
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						OrgID: &o1.ID,
						Type:  influxdb.DocumentsResourceType,
					},
				},
			},
		}

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
				Labels:        []*influxdb.Label{l1},
				Organizations: map[influxdb.ID]influxdb.UserType{o1.ID: influxdb.Owner},
			}
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s1)

			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 0, 0, time.UTC)
			if err := s.CreateDocument(ctx, d1); err != nil {
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
				Labels:        []*influxdb.Label{l2},
				Organizations: map[influxdb.ID]influxdb.UserType{o1.ID: influxdb.Owner},
			}
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s2)

			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 1, 0, time.UTC)
			if err := s.CreateDocument(ctx, d2); err == nil {
				t.Fatalf("should not have been authorized to create document")
			}

			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 1, 0, time.UTC)
			d2.Organizations = map[influxdb.ID]influxdb.UserType{o2.ID: influxdb.Owner}
			if err := s.CreateDocument(ctx, d2); err != nil {
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
				Organizations: map[influxdb.ID]influxdb.UserType{o2.ID: influxdb.Owner},
			}
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s1)

			mockTimeGen.FakeValue = time.Date(2009, 1, 2, 3, 0, 2, 0, time.UTC)
			if err := s.CreateDocument(ctx, d3); err == nil {
				t.Errorf("should not have be authorized to create document")
			}
		})

		t.Run("can't create document with non existing label", func(t *testing.T) {
			d4 := &influxdb.Document{
				Meta: influxdb.DocumentMeta{
					Name: "i4",
				},
				Content: map[string]interface{}{
					"k4": "v4",
				},
				Labels:        []*influxdb.Label{lBad},
				Organizations: map[influxdb.ID]influxdb.UserType{o1.ID: influxdb.Owner},
			}
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s1)
			err = s.CreateDocument(ctx, d4)
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

		t.Run("u1 can see only o1s documents by label", func(t *testing.T) {
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s1)
			ds, err := ss.FindDocuments(
				ctx,
				influxdb.WhereOrgID(o1.ID),
				influxdb.WhereOrgID(o2.ID),
				influxdb.IncludeContent,
				influxdb.IncludeLabels,
			)
			if err != nil {
				t.Fatalf("failed to retrieve documents: %v", err)
			}

			if exp, got := []*influxdb.Document{dl1}, ds; !docsEqual(got, exp) {
				t.Errorf("documents are different -got/+want\ndiff %s", docsDiff(got, exp))
			}
		})

		t.Run("check not found err", func(t *testing.T) {
			_, err := ss.FindDocument(ctx, MustIDBase16(fourID))
			ErrorsEqual(t, err, &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  influxdb.ErrDocumentNotFound,
			})
		})

		t.Run("u2 can see o1 and o2s documents", func(t *testing.T) {
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s2)
			ds, err := ss.FindDocuments(
				ctx,
				influxdb.WhereOrgID(o1.ID),
				influxdb.WhereOrgID(o2.ID),
				influxdb.IncludeContent,
				influxdb.IncludeLabels,
			)
			if err != nil {
				t.Fatalf("failed to retrieve documents for org1: %v", err)
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
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s2)
			if err := s.UpdateDocument(ctx, d); err == nil {
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
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s2)
			if err := s.UpdateDocument(ctx, d); err != nil {
				t.Errorf("unexpected error updating document: %v", err)
			}
		})

		t.Run("u1 can update document d1", func(t *testing.T) {
			ctx := context.Background()
			ctx = icontext.SetAuthorizer(ctx, s1)
			if err := s.DeleteDocuments(ctx); err != nil {
				t.Errorf("unexpected error deleteing document: %v", err)
			}
		})

	}
}

func docsEqual(i1, i2 interface{}) bool {
	return cmp.Equal(i1, i2, documentCmpOptions...)
}

func docsDiff(i1, i2 interface{}) string {
	return cmp.Diff(i1, i2, documentCmpOptions...)
}

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
