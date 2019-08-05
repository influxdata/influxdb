package http_test

import (
	"context"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/servicetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestTaskService(t *testing.T) {
	t.Parallel()
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {

			service := kv.NewService(inmem.NewKVStore())
			ctx, cancelFunc := context.WithCancel(context.Background())

			if err := service.Initialize(ctx); err != nil {
				t.Fatalf("error initializing urm service: %v", err)
			}

			h := http.NewAuthenticationHandler(http.ErrorHandler(0))
			h.AuthorizationService = service
			th := http.NewTaskHandler(&http.TaskBackend{
				HTTPErrorHandler:           http.ErrorHandler(0),
				Logger:                     zaptest.NewLogger(t).With(zap.String("handler", "task")),
				TaskService:                service,
				AuthorizationService:       service,
				OrganizationService:        service,
				UserResourceMappingService: service,
				LabelService:               service,
				UserService:                service,
				BucketService:              service,
			})
			h.Handler = th

			org := &platform.Organization{Name: t.Name() + "_org"}
			if err := service.CreateOrganization(ctx, org); err != nil {
				t.Fatal(err)
			}
			user := &platform.User{Name: t.Name() + "_user"}
			if err := service.CreateUser(ctx, user); err != nil {
				t.Fatal(err)
			}
			auth := platform.Authorization{UserID: user.ID, OrgID: org.ID}
			if err := service.CreateAuthorization(ctx, &auth); err != nil {
				t.Fatal(err)
			}

			server := httptest.NewServer(h)
			go func() {
				<-ctx.Done()
				server.Close()
			}()

			taskService := http.TaskService{
				Addr:  server.URL,
				Token: auth.Token,
			}

			cFunc := func(t *testing.T) (servicetest.TestCreds, error) {
				org := &platform.Organization{Name: t.Name() + "_org"}
				if err := service.CreateOrganization(ctx, org); err != nil {
					t.Fatal(err)
				}

				return servicetest.TestCreds{
					OrgID:           org.ID,
					Org:             org.Name,
					UserID:          user.ID,
					AuthorizationID: auth.ID,
					Token:           auth.Token,
				}, nil
			}

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        taskService,
				I:                  service,
				Ctx:                ctx,
				CredsFunc:          cFunc,
			}, cancelFunc
		},
		"transactional",
	)
}
