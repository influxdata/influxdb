package organization

import (
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/testhelper"
	"github.com/stretchr/testify/assert"
)

func Test_Org_Basic(t *testing.T) {
	db := testhelper.NewTestBoltDb(t)
	defer db.Close()
	assert.Equal(t, `ID			Name
dd7cd2292f6e974a	myorg
`,
		testhelper.MustRunCommand(t, NewOrgCommand(), "list", "--bolt-path", db.Name()))

	// org creation only works for new names
	assert.EqualError(t, testhelper.RunCommand(t, NewOrgCommand(), "create", "--bolt-path", db.Name(), "--org", "myorg"), "organization with name myorg already exists")

	// org creation works
	assert.NoError(t, testhelper.RunCommand(t, NewOrgCommand(), "create", "--bolt-path", db.Name(), "--org", "neworg"))

	// neworg shows up in list of orgs
	assert.Regexp(t, "\tneworg\n", testhelper.MustRunCommand(t, NewOrgCommand(), "list", "--bolt-path", db.Name()))
}
