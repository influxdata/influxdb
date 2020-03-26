package flight

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRequest(t *testing.T) {
	data := `{ "org_id": "0440e2fda1557000", "bucket_id": "ecd99cb93e30b221", "start": "2020-01-01T00:00:00Z" }`

	var got GetRequest
	err := json.Unmarshal([]byte(data), &got)
	assert.NoError(t, err)
}
