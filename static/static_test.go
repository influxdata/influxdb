package static

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEtag(t *testing.T) {
	testTime := time.Time{}

	testTime = testTime.Add(26 * time.Hour)
	testTime = testTime.Add(15 * time.Minute)
	testTime = testTime.Add(20 * time.Second)

	testSize := int64(1500)

	got := etag(testSize, testTime)
	want := `"1500221520"`

	require.Equal(t, got, want)
}
