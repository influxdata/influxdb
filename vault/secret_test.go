// +build integration

package vault_test

import (
	"context"
	"fmt"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/influxdata/influxdb/vault"
	testcontainer "github.com/testcontainers/testcontainer-go"
)

func initSecretService(f platformtesting.SecretServiceFields, t *testing.T) (platform.SecretService, func()) {
	token := "test"
	ctx := context.Background()
	vaultC, err := testcontainer.RunContainer(ctx, "vault", testcontainer.RequestContainer{
		ExportedPort: []string{
			"8200/tcp",
		},
		Cmd: fmt.Sprintf(`vault server -dev -dev-listen-address 0.0.0.0:8200 -dev-root-token-id=%s`, token),
	})
	if err != nil {
		t.Fatalf("failed to initialize vault testcontiner: %v", err)
	}
	ip, port, err := vaultC.GetHostEndpoint(ctx, "8200/tcp")
	if err != nil {
		t.Fatal(err)
	}

	s, err := vault.NewSecretService()
	if err != nil {
		t.Fatal(err)
	}
	s.Client.SetToken(token)
	s.Client.SetAddress(fmt.Sprintf("http://%v:%v", ip, port))

	for _, sec := range f.Secrets {
		for k, v := range sec.Env {
			if err := s.PutSecret(ctx, sec.OrganizationID, k, v); err != nil {
				t.Fatalf("failed to populate secrets: %v", err)
			}
		}
	}
	return s, func() {
		defer vaultC.Terminate(ctx, t)
	}
}

func TestSecretService(t *testing.T) {
	platformtesting.SecretService(initSecretService, t)
}
