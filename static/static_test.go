package static

import (
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAssetHandler(t *testing.T) {
	t.Parallel()

	defaultData := []byte("this is the default file")
	otherData := []byte("this is a different file")

	m := http.FS(fstest.MapFS{
		defaultFile: {
			Data:    defaultData,
			ModTime: time.Now(),
		},
		"somethingElse.js": {
			Data:    otherData,
			ModTime: time.Now(),
		},
	})

	tests := []struct {
		name     string
		reqPath  string
		newPath  string
		wantData []byte
	}{
		{
			name:     "path matches default",
			reqPath:  "/" + defaultFile,
			newPath:  "/" + defaultFile,
			wantData: defaultData,
		},
		{
			name:     "root path",
			reqPath:  "/",
			newPath:  "/" + defaultFile,
			wantData: defaultData,
		},
		{
			name:     "path matches a file",
			reqPath:  "/somethingElse.js",
			newPath:  "/somethingElse.js",
			wantData: otherData,
		},
		{
			name:     "path matches nothing",
			reqPath:  "/something_random",
			newPath:  fallbackPathSlug,
			wantData: defaultData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := assetHandler(m)
			r := httptest.NewRequest("GET", tt.reqPath, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			b, err := io.ReadAll(w.Result().Body)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, w.Result().StatusCode)
			require.Equal(t, tt.wantData, b)
			require.Equal(t, tt.newPath, r.URL.Path)
		})
	}
}

func TestModTimeFromInfo(t *testing.T) {
	t.Parallel()

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
		name     string
		file     string
		fallback bool
		want     []byte
	}{
		{
			"default file by name",
			defaultFile,
			false,
			defaultData,
		},
		{
			"other file by name",
			"somethingElse.js",
			false,
			otherData,
		},
		{
			"falls back to default if can't find",
			"badFile.exe",
			true,
			defaultData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFile, fallback, err := openAsset(m, tt.file)
			require.NoError(t, err)
			require.Equal(t, tt.fallback, fallback)

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
