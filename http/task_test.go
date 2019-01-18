package http_test

import (
	"context"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task"
	"github.com/influxdata/influxdb/task/backend"
	tmock "github.com/influxdata/influxdb/task/mock"
	"github.com/influxdata/influxdb/task/servicetest"
	"go.uber.org/zap/zaptest"
)

func httpTaskServiceFactory(t *testing.T) (*servicetest.System, context.CancelFunc) {
	store := backend.NewInMemStore()
	rrw := backend.NewInMemRunReaderWriter()
	sch := tmock.NewScheduler()

	ctx, cancel := context.WithCancel(context.Background())

	backingTS := task.PlatformAdapter(store, rrw, sch)

	i := inmem.NewService()

	h := http.NewAuthenticationHandler()
	h.AuthorizationService = i
	th := http.NewTaskHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), zaptest.NewLogger(t), mock.NewUserService())
	th.OrganizationService = &mock.OrganizationService{
		FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*platform.Organization, error) {
			return &platform.Organization{ID: id, Name: "test"}, nil
		},
		FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
			org := &platform.Organization{}
			if filter.Name != nil {
				org.Name = *filter.Name
			}
			if filter.ID != nil {
				org.ID = *filter.ID
			}

			return org, nil
		},
	}
	th.TaskService = backingTS
	th.AuthorizationService = i
	h.Handler = th

	org := &platform.Organization{Name: t.Name() + "_org"}
	if err := i.CreateOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	user := &platform.User{Name: t.Name() + "_user"}
	if err := i.CreateUser(ctx, user); err != nil {
		t.Fatal(err)
	}
	auth := platform.Authorization{UserID: user.ID, OrgID: org.ID}
	if err := i.CreateAuthorization(ctx, &auth); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(h)
	go func() {
		<-ctx.Done()
		server.Close()
	}()

	tsFunc := func() platform.TaskService {
		return http.TaskService{
			Addr:  server.URL,
			Token: auth.Token,
		}
	}

	cFunc := func() (o, u platform.ID, tok string, err error) {
		return org.ID, user.ID, auth.Token, nil
	}

	return &servicetest.System{
		S:               store,
		LR:              rrw,
		LW:              rrw,
		Ctx:             ctx,
		TaskServiceFunc: tsFunc,
		CredsFunc:       cFunc,
	}, cancel
}

func TestTaskService(t *testing.T) {
	t.Parallel()

	servicetest.TestTaskService(t, httpTaskServiceFactory)
}
