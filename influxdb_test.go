package influxdb

import (
	"testing"
)

func TestClient(t *testing.T) {
	client, err := NewClient(&ClientConfig{})
	if err != nil {
		t.Error(err)
	}

	if err := client.CreateClusterAdmin("admin", "password"); err != nil {
		t.Error(err)
	}

	admins, err := client.GetClusterAdminList()
	if err != nil {
		t.Error(err)
	}

	if len(admins) != 2 {
		t.Error("more than two admins returned")
	}

	if err := client.CreateDatabase("foobar"); err != nil {
		t.Error(err)
	}

	dbs, err := client.GetDatabaseList()
	if err != nil {
		t.Error(err)
	}

	if len(dbs) != 1 && dbs[0]["foobar"] == nil {
		t.Errorf("List of databases don't match")
	}

	if err := client.CreateDatabaseUser("foobar", "dbuser", "pass"); err != nil {
		t.Error(err)
	}

	if err := client.AlterDatabasePrivilege("foobar", "dbuser", true); err != nil {
		t.Error(err)
	}

	users, err := client.GetDatabaseUserList("foobar")
	if err != nil {
		t.Error(err)
	}

	if len(users) != 1 {
		t.Error("more than one user returned")
	}

	client, err = NewClient(&ClientConfig{
		Username: "dbuser",
		Password: "pass",
		Database: "foobar",
	})

	if err != nil {
		t.Error(err)
	}

	series := &Series{
		Name:    "ts9",
		Columns: []string{"value"},
		Points: [][]interface{}{
			[]interface{}{1.0},
		},
	}
	if err := client.WriteSeries([]*Series{series}); err != nil {
		t.Error(err)
	}

	result, err := client.Query("select * from ts9")
	if err != nil {
		t.Error(err)
	}

	if len(result) != 1 {
		t.Error("more than one time series returned")
	}

	if len(result[0].Points) != 1 {
		t.Error("Larger time series returned")
	}

	if result[0].Points[0][2].(float64) != 1 {
		t.Fail()
	}
}
