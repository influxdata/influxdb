//go:build integration

package vault_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxdb/v2/vault"
	testcontainers "github.com/testcontainers/testcontainers-go"
)

func initSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	token := "test"
	ctx := context.Background()
	vaultC, err := GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "docker.io/vault:latest",
			ExposedPorts: []string{
				"8200/tcp",
			},
			Cmd: fmt.Sprintf(`vault server -dev -dev-listen-address 0.0.0.0:8200 -dev-root-token-id=%s`, token),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to initialize vault container: %v", err)
	}

	host, err := vaultC.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host from vault container: %v", err)
	}

	port, err := vaultC.MappedPort(ctx, "8200/tcp")
	if err != nil {
		t.Fatalf("failed to get exposed 8200 port from vault container: %v", err)
	}

	s, err := vault.NewSecretService()
	if err != nil {
		t.Fatal(err)
	}
	s.Client.SetToken(token)
	s.Client.SetAddress(fmt.Sprintf("http://%v:%v", host, port.Int()))

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
	influxdbtesting.SecretService(initSecretService, t)
}
