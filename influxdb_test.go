package influxdb

import (
	"testing"
)

func TestClient(t *testing.T) {
	internalTest(t, true)
}

func TestClientWithoutCompression(t *testing.T) {
	internalTest(t, false)
}

func internalTest(t *testing.T, compression bool) {
	client, err := NewClient(&ClientConfig{})
	if err != nil {
		t.Error(err)
	}

	admins, err := client.GetClusterAdminList()
	if err != nil {
		t.Error(err)
	}

	if len(admins) == 1 {
		if err := client.CreateClusterAdmin("admin", "password"); err != nil {
			t.Error(err)
		}
	}

	admins, err = client.GetClusterAdminList()
	if err != nil {
		t.Error(err)
	}

	if len(admins) != 2 {
		t.Error("more than two admins returned")
	}

	dbs, err := client.GetDatabaseList()
	if err != nil {
		t.Error(err)
	}

	if len(dbs) == 0 {
		if err := client.CreateDatabase("foobar"); err != nil {
			t.Error(err)
		}
	}

	dbs, err = client.GetDatabaseList()
	if err != nil {
		t.Error(err)
	}

	if len(dbs) != 1 && dbs[0]["foobar"] == nil {
		t.Errorf("List of databases don't match")
	}

	users, err := client.GetDatabaseUserList("foobar")
	if err != nil {
		t.Error(err)
	}

	if len(users) == 0 {
		if err := client.CreateDatabaseUser("foobar", "dbuser", "pass"); err != nil {
			t.Error(err)
		}

		if err := client.AlterDatabasePrivilege("foobar", "dbuser", true); err != nil {
			t.Error(err)
		}
	}

	users, err = client.GetDatabaseUserList("foobar")
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

	if !compression {
		client.DisableCompression()
	}

	if err != nil {
		t.Error(err)
	}

	name := "ts9"
	if !compression {
		name = "ts9_uncompressed"
	}

	series := &Series{
		Name:    name,
		Columns: []string{"value"},
		Points: [][]interface{}{
			[]interface{}{1.0},
		},
	}
	if err := client.WriteSeries([]*Series{series}); err != nil {
		t.Error(err)
	}

	result, err := client.Query("select * from " + name)
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

	client, err = NewClient(&ClientConfig{
		Username: "root",
		Password: "root",
	})

	if err != nil {
		t.Error(err)
	}

	shards, err := client.GetShards()
	if shards == nil || len(shards.ShortTerm) == 0 || err != nil {
		t.Error("There were no shards in the db: ", err)
	}

	err = client.DropShard(shards.ShortTerm[0].Id, []uint32{uint32(1)})
	if err != nil {
		t.Error(err)
	}
}
