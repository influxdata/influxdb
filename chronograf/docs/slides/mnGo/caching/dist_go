// +build OMIT
package dist

import (
	"fmt"
	"net/http"
)

func (b *BindataAssets) addCacheHeaders(filename string, w http.ResponseWriter) error {
	w.Header().Add("Cache-Control", "public, max-age=3600")
	fi, _ := AssetInfo(filename)
	hour, minute, second := fi.ModTime().Clock()
	etag := fmt.Sprintf(`"%d%d%d%d%d"`, fi.Size(), fi.ModTime().Day(), hour, minute, second)
	w.Header().Set("ETag", etag)
}
