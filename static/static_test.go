package static

import (
	"io/fs"
	"io/ioutil"
	"net/http"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModTimeFromInfo(t *testing.T) {
	nowTime := time.Now()

	timeFunc := func() (time.Time, error) {
		return nowTime, nil
	}

	fsys := fstest.MapFS{
		"zeroTime.file": {
			ModTime: time.Time{},
		},
		"notZeroTime.file": {
			ModTime: nowTime,
		},
	}

	info1, err := fsys.Stat("zeroTime.file")
	require.NoError(t, err)

	info2, err := fsys.Stat("notZeroTime.file")
	require.NoError(t, err)

	tests := []struct {
		name string
		info fs.FileInfo
		want time.Time
	}{
		{
			"zero time returns fallback time",
			info1,
			nowTime,
		},
		{
			"non-zero time returns same time",
			info2,
			nowTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := modTimeFromInfo(tt.info, timeFunc)

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOpenAsset(t *testing.T) {
	t.Parallel()

	defaultData := []byte("this is the default file")
	otherData := []byte("this is a different file")

	m := http.FS(fstest.MapFS{
		defaultFile: {
			Data: defaultData,
		},
		"somethingElse.js": {
			Data: otherData,
		},
	})

	tests := []struct {
		name string
		file string
		want []byte
	}{
		{
			"default file by name",
			defaultFile,
			defaultData,
		},
		{
			"other file by name",
			"somethingElse.js",
			otherData,
		},
		{
			"falls back to default if can't find",
			"badFile.exe",
			defaultData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFile, err := openAsset(m, tt.file)
			require.NoError(t, err)

			got, err := ioutil.ReadAll(gotFile)
			require.NoError(t, err)

			require.Equal(t, tt.want, got)
		})
	}

}

func TestEtag(t *testing.T) {
	t.Parallel()

	testTime := time.Time{}

	testTime = testTime.Add(26 * time.Hour)
	testTime = testTime.Add(15 * time.Minute)
	testTime = testTime.Add(20 * time.Second)

	testSize := int64(1500)

	got := etag(testSize, testTime)
	want := `"1500221520"`

	require.Equal(t, got, want)
}
