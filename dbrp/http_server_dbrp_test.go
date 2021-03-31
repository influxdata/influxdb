package dbrp_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func initHttpService(t *testing.T) (influxdb.DBRPMappingServiceV2, *httptest.Server, func()) {
	t.Helper()
	ctx := context.Background()
	bucketSvc := mock.NewBucketService()
	orgSvc := &mock.OrganizationService{
		FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
			if filter.Name == nil || *filter.Name != "org" {
				return nil, errors.New("not found")
			}
			return &influxdb.Organization{
				Name: "org",
				ID:   influxdbtesting.MustIDBase16("059af7ed2a034000"),
			}, nil
		},
	}

	s, closeS, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatal(err)
	}

	svc := dbrp.NewService(ctx, bucketSvc, s)

	server := httptest.NewServer(dbrp.NewHTTPHandler(zaptest.NewLogger(t), svc, orgSvc))
	return svc, server, func() {
		closeS()
		server.Close()
	}
}

func Test_handlePostDBRP(t *testing.T) {
	table := []struct {
		Name         string
		ExpectedErr  error
		ExpectedDBRP *influxdb.DBRPMappingV2
		Input        io.Reader
	}{
		{
			Name: "Create valid dbrp",
			Input: strings.NewReader(`{
	"bucketID": "5555f7ed2a035555",
	"orgID": "059af7ed2a034000",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedDBRP: &influxdb.DBRPMappingV2{
				OrganizationID: influxdbtesting.MustIDBase16("059af7ed2a034000"),
			},
		},
		{
			Name: "Create valid dbrp by org name",
			Input: strings.NewReader(`{
	"bucketID": "5555f7ed2a035555",
	"org": "org",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedDBRP: &influxdb.DBRPMappingV2{
				OrganizationID: influxdbtesting.MustIDBase16("059af7ed2a034000"),
			},
		},
		{
			Name: "Create with no orgID or org",
			Input: strings.NewReader(`{
	"bucketID": "5555f7ed2a035555",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedErr: dbrp.ErrNoOrgProvided,
		},
		{
			Name: "Create with invalid orgID",
			Input: strings.NewReader(`{
	"bucketID": "5555f7ed2a035555",
	"orgID": "invalid",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedErr: dbrp.ErrInvalidOrgID("invalid", platform.ErrInvalidIDLength),
		},
		{
			Name: "Create with invalid org name",
			Input: strings.NewReader(`{
	"bucketID": "5555f7ed2a035555",
	"org": "invalid",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedErr: dbrp.ErrOrgNotFound("invalid"),
		},
		{
			Name: "Create with invalid bucket ID",
			Input: strings.NewReader(`{
	"bucketID": "invalid",
	"org": "org",
	"database": "mydb",
	"retention_policy": "autogen",
	"default": false
}`),
			ExpectedErr: dbrp.ErrInvalidBucketID("invalid", platform.ErrInvalidIDLength),
		},
	}

	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.ExpectedErr != nil && tt.ExpectedDBRP != nil {
				t.Fatal("one of `ExpectedErr` or `ExpectedDBRP` has to be set")
			}
			_, server, shutdown := initHttpService(t)
			defer shutdown()
			client := server.Client()

			resp, err := client.Post(server.URL+"/", "application/json", tt.Input)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if tt.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				var actualErr errors2.Error
				if err := json.Unmarshal(b, &actualErr); err != nil {
					t.Fatal(err)
				}

				assert.Equal(t, tt.ExpectedErr.Error(), actualErr.Error())
				return
			}
			dbrp := &influxdb.DBRPMappingV2{}
			if err := json.NewDecoder(resp.Body).Decode(&dbrp); err != nil {
				t.Fatal(err)
			}

			if !dbrp.ID.Valid() {
				t.Fatalf("expected valid id, got an invalid one %s", dbrp.ID.String())
			}

			if dbrp.OrganizationID != tt.ExpectedDBRP.OrganizationID {
				t.Fatalf("expected orgid %s got %s", tt.ExpectedDBRP.OrganizationID, dbrp.OrganizationID)
			}

			if !dbrp.Default {
				t.Fatalf("expected dbrp to be marked as default")
			}
		})
	}
}

func Test_handleGetDBRPs(t *testing.T) {
	table := []struct {
		Name          string
		QueryParams   string
		ExpectedErr   error
		ExpectedDBRPs []influxdb.DBRPMappingV2
	}{
		{
			Name:        "ok",
			QueryParams: "orgID=059af7ed2a034000",
			ExpectedDBRPs: []influxdb.DBRPMappingV2{
				{
					ID:              influxdbtesting.MustIDBase16("1111111111111111"),
					BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
					OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
					Database:        "mydb",
					RetentionPolicy: "autogen",
					Default:         true,
				},
			},
		},
		{
			Name:        "invalid org",
			QueryParams: "orgID=invalid",
			ExpectedErr: dbrp.ErrInvalidOrgID("invalid", platform.ErrInvalidIDLength),
		},
		{
			Name:        "invalid bucket",
			QueryParams: "orgID=059af7ed2a034000&bucketID=invalid",
			ExpectedErr: dbrp.ErrInvalidBucketID("invalid", platform.ErrInvalidIDLength),
		},
		{
			Name:        "invalid default",
			QueryParams: "orgID=059af7ed2a034000&default=notabool",
			ExpectedErr: &errors2.Error{
				Code: errors2.EInvalid,
				Msg:  "invalid default parameter",
			},
		},
		{
			Name:        "no org",
			QueryParams: "default=true&retention_police=lol",
			ExpectedErr: dbrp.ErrNoOrgProvided,
		},
		{
			Name:          "no match",
			QueryParams:   "orgID=059af7ed2a034000&default=false",
			ExpectedDBRPs: []influxdb.DBRPMappingV2{},
		},
		{
			Name:        "all match",
			QueryParams: "orgID=059af7ed2a034000&default=true&rp=autogen&db=mydb&bucketID=5555f7ed2a035555&id=1111111111111111",
			ExpectedDBRPs: []influxdb.DBRPMappingV2{
				{
					ID:              influxdbtesting.MustIDBase16("1111111111111111"),
					BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
					OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
					Database:        "mydb",
					RetentionPolicy: "autogen",
					Default:         true,
				},
			},
		},
		{
			Name:        "org name",
			QueryParams: "org=org",
			ExpectedDBRPs: []influxdb.DBRPMappingV2{
				{
					ID:              influxdbtesting.MustIDBase16("1111111111111111"),
					BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
					OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
					Database:        "mydb",
					RetentionPolicy: "autogen",
					Default:         true,
				},
			},
		},
	}

	ctx := context.Background()
	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.ExpectedErr != nil && len(tt.ExpectedDBRPs) != 0 {
				t.Error("one of those has to be set")
			}
			svc, server, shutdown := initHttpService(t)
			defer shutdown()

			if svc, ok := svc.(*dbrp.Service); ok {
				svc.IDGen = mock.NewIDGenerator("1111111111111111", t)
			}
			db := &influxdb.DBRPMappingV2{
				BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, db); err != nil {
				t.Fatal(err)
			}

			client := server.Client()
			resp, err := client.Get(server.URL + "?" + tt.QueryParams)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if tt.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				var actualErr errors2.Error
				if err := json.Unmarshal(b, &actualErr); err != nil {
					t.Fatal(err)
				}

				assert.Equal(t, tt.ExpectedErr.Error(), actualErr.Error())
				return
			}
			dbrps := struct {
				Content []influxdb.DBRPMappingV2 `json:"content"`
			}{}
			if err := json.NewDecoder(resp.Body).Decode(&dbrps); err != nil {
				t.Fatal(err)
			}

			if len(dbrps.Content) != len(tt.ExpectedDBRPs) {
				t.Fatalf("expected %d dbrps got %d", len(tt.ExpectedDBRPs), len(dbrps.Content))
			}

			if !cmp.Equal(tt.ExpectedDBRPs, dbrps.Content) {
				t.Fatalf(cmp.Diff(tt.ExpectedDBRPs, dbrps.Content))
			}

		})
	}
}

func Test_handlePatchDBRP(t *testing.T) {
	table := []struct {
		Name         string
		ExpectedErr  error
		ExpectedDBRP *influxdb.DBRPMappingV2
		URLSuffix    string
		Input        io.Reader
	}{
		{
			Name:      "happy path update",
			URLSuffix: "/1111111111111111?orgID=059af7ed2a034000",
			Input: strings.NewReader(`{
	"retention_policy": "updaterp",
	"database": "wont_change"
}`),
			ExpectedDBRP: &influxdb.DBRPMappingV2{
				ID:              influxdbtesting.MustIDBase16("1111111111111111"),
				BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "updaterp",
				Default:         true,
			},
		},
		{
			Name:      "happy path update by org name",
			URLSuffix: "/1111111111111111?org=org",
			Input: strings.NewReader(`{
	"retention_policy": "updaterp",
	"database": "wont_change"
}`),
			ExpectedDBRP: &influxdb.DBRPMappingV2{
				ID:              influxdbtesting.MustIDBase16("1111111111111111"),
				BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "updaterp",
				Default:         true,
			},
		},
		{
			Name:      "invalid org",
			URLSuffix: "/1111111111111111?orgID=invalid",
			Input: strings.NewReader(`{
	"database": "updatedb"
}`),
			ExpectedErr: dbrp.ErrInvalidOrgID("invalid", platform.ErrInvalidIDLength),
		},
		{
			Name:      "no org",
			URLSuffix: "/1111111111111111",
			Input: strings.NewReader(`{
	"database": "updatedb"
}`),
			ExpectedErr: dbrp.ErrNoOrgProvided,
		},
		{
			Name:        "not found",
			URLSuffix:   "/1111111111111111?orgID=059af7ed2a034001",
			ExpectedErr: dbrp.ErrDBRPNotFound,
		},
	}

	ctx := context.Background()

	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.ExpectedErr != nil && tt.ExpectedDBRP != nil {
				t.Error("one of those has to be set")
			}
			svc, server, shutdown := initHttpService(t)
			defer shutdown()
			client := server.Client()

			if svc, ok := svc.(*dbrp.Service); ok {
				svc.IDGen = mock.NewIDGenerator("1111111111111111", t)
			}

			dbrp := &influxdb.DBRPMappingV2{
				BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, dbrp); err != nil {
				t.Fatal(err)
			}

			req, _ := http.NewRequest(http.MethodPatch, server.URL+tt.URLSuffix, tt.Input)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if tt.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				var actualErr errors2.Error
				if err := json.Unmarshal(b, &actualErr); err != nil {
					t.Fatal(err)
				}

				assert.Equal(t, tt.ExpectedErr.Error(), actualErr.Error())
				return
			}
			dbrpResponse := struct {
				Content *influxdb.DBRPMappingV2 `json:"content"`
			}{}

			if err := json.NewDecoder(resp.Body).Decode(&dbrpResponse); err != nil {
				t.Fatal(err)
			}

			if !cmp.Equal(tt.ExpectedDBRP, dbrpResponse.Content) {
				t.Fatalf(cmp.Diff(tt.ExpectedDBRP, dbrpResponse.Content))
			}
		})
	}
}

func Test_handleDeleteDBRP(t *testing.T) {
	table := []struct {
		Name              string
		URLSuffix         string
		ExpectedErr       error
		ExpectStillExists bool
	}{
		{
			Name:      "delete",
			URLSuffix: "/1111111111111111?orgID=059af7ed2a034000",
		},
		{
			Name:      "delete by org name",
			URLSuffix: "/1111111111111111?org=org",
		},
		{
			Name:        "invalid org",
			URLSuffix:   "/1111111111111111?orgID=invalid",
			ExpectedErr: dbrp.ErrInvalidOrgID("invalid", platform.ErrInvalidIDLength),
		},
		{
			Name:        "invalid org name",
			URLSuffix:   "/1111111111111111?org=invalid",
			ExpectedErr: dbrp.ErrOrgNotFound("invalid"),
		},
		{
			Name:        "no org",
			URLSuffix:   "/1111111111111111",
			ExpectedErr: dbrp.ErrNoOrgProvided,
		},
		{
			// Not found is not an error for Delete.
			Name:              "not found",
			URLSuffix:         "/1111111111111111?orgID=059af7ed2a034001",
			ExpectStillExists: true,
		},
	}

	ctx := context.Background()

	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			svc, server, shutdown := initHttpService(t)
			defer shutdown()
			client := server.Client()

			d := &influxdb.DBRPMappingV2{
				ID:              influxdbtesting.MustIDBase16("1111111111111111"),
				BucketID:        influxdbtesting.MustIDBase16("5555f7ed2a035555"),
				OrganizationID:  influxdbtesting.MustIDBase16("059af7ed2a034000"),
				Database:        "mydb",
				RetentionPolicy: "autogen",
				Default:         true,
			}
			if err := svc.Create(ctx, d); err != nil {
				t.Fatal(err)
			}

			req, _ := http.NewRequest(http.MethodDelete, server.URL+tt.URLSuffix, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if tt.ExpectedErr != nil {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				var actualErr errors2.Error
				if err := json.Unmarshal(b, &actualErr); err != nil {
					t.Fatal(err)
				}

				assert.Equal(t, tt.ExpectedErr.Error(), actualErr.Error())
				return
			}

			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("expected status code %d, got %d", http.StatusNoContent, resp.StatusCode)
			}

			gotDBRP, err := svc.FindByID(ctx, influxdbtesting.MustIDBase16("059af7ed2a034000"), influxdbtesting.MustIDBase16("1111111111111111"))
			if tt.ExpectStillExists {
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(d, gotDBRP); diff != "" {
					t.Fatal(diff)
				}
			} else {
				if err == nil {
					t.Fatal("expected error got none")
				}
				if !errors.Is(err, dbrp.ErrDBRPNotFound) {
					t.Fatalf("expected err dbrp not found, got %s", err)
				}
			}
		})
	}
}
