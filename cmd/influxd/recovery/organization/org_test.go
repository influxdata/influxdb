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

	// org name not created unless create argument given
	assert.NoError(t, testhelper.RunCommand(t, NewOrgCommand(), "create", "--bolt-path", db.Name(), "--org", "neworg"))

	assert.Regexp(t, `ID			Name
[^\t]*	neworg
dd7cd2292f6e974a	myorg
`,
		testhelper.MustRunCommand(t, NewOrgCommand(), "list", "--bolt-path", db.Name()))
}
