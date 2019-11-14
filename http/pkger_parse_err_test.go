package http

import (
	"testing"

	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PkgValidationErr(t *testing.T) {
	iPtr := func(i int) *int {
		return &i
	}

	compIntSlcs := func(t *testing.T, expected []int, actuals []*int) {
		t.Helper()

		if len(expected) >= len(actuals) {
			require.FailNow(t, "expected array is larger than actuals")
		}

		for i, actual := range actuals {
			if i == len(expected) {
				assert.Nil(t, actual)
				continue
			}
			assert.Equal(t, expected[i], *actual)
		}
	}

	pErr := &pkger.ParseErr{
		Resources: []pkger.ResourceErr{
			{
				Kind: pkger.KindDashboard.String(),
				Idx:  0,
				ValidationErrs: []pkger.ValidationErr{
					{
						Field: "charts",
						Index: iPtr(1),
						Nested: []pkger.ValidationErr{
							{
								Field: "colors",
								Index: iPtr(0),
								Nested: []pkger.ValidationErr{
									{
										Field: "hex",
										Msg:   "hex value required",
									},
								},
							},
							{
								Field: "kind",
								Msg:   "chart kind must be provided",
							},
						},
					},
				},
			},
		},
	}

	errs := convertParseErr(pErr)
	require.Len(t, errs, 2)
	assert.Equal(t, pkger.KindDashboard.String(), errs[0].Kind)
	assert.Equal(t, []string{"resources", "charts", "colors", "hex"}, errs[0].Fields)
	compIntSlcs(t, []int{0, 1, 0}, errs[0].Indexes)
	assert.Equal(t, "hex value required", errs[0].Reason)

	assert.Equal(t, pkger.KindDashboard.String(), errs[1].Kind)
	assert.Equal(t, []string{"resources", "charts", "kind"}, errs[1].Fields)
	compIntSlcs(t, []int{0, 1}, errs[1].Indexes)
	assert.Equal(t, "chart kind must be provided", errs[1].Reason)
}
