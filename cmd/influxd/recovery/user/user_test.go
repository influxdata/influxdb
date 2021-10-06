package user

import (
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/testhelper"
	"github.com/stretchr/testify/assert"
)

func Test_User_Basic(t *testing.T) {
	db := testhelper.NewTestBoltDb(t)
	defer db.Close()
	assert.Equal(t, `ID			Name
08371db1dd8c8000	testuser
`,
		testhelper.MustRunCommand(t, NewUserCommand(), "list", "--bolt-path", db.Name()))

	// org name not created unless create argument given
	assert.NoError(t, testhelper.RunCommand(t, NewUserCommand(), "create", "--bolt-path", db.Name(), "--username", "testuser2"))

	assert.Regexp(t, "\ttestuser2\n",
		testhelper.MustRunCommand(t, NewUserCommand(), "list", "--bolt-path", db.Name()))
}
