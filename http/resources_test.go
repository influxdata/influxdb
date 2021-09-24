package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
)

func TestResourceListHandler(t *testing.T) {
	w := httptest.NewRecorder()

	NewResourceListHandler().ServeHTTP(w,
		httptest.NewRequest(
			"GET",
			"http://howdy.tld/api/v2/resources",
			nil,
		),
	)

	expectedResponse := []string{
		string(influxdb.AuthorizationsResourceType),
		string(influxdb.BucketsResourceType),
		string(influxdb.DashboardsResourceType),
		string(influxdb.OrgsResourceType),
		string(influxdb.SourcesResourceType),
		string(influxdb.TasksResourceType),
		string(influxdb.TelegrafsResourceType),
		string(influxdb.UsersResourceType),
		string(influxdb.VariablesResourceType),
		string(influxdb.ScraperResourceType),
		string(influxdb.SecretsResourceType),
		string(influxdb.LabelsResourceType),
		string(influxdb.ViewsResourceType),
		string(influxdb.DocumentsResourceType),
		string(influxdb.NotificationRuleResourceType),
		string(influxdb.NotificationEndpointResourceType),
		string(influxdb.ChecksResourceType),
		string(influxdb.DBRPResourceType),
	}

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Logf(string(body))
		t.Errorf("unexpected status: %s", resp.Status)
	}

	var actualReponse []string
	if err := json.Unmarshal(body, &actualReponse); err != nil {
		t.Errorf("unexpected response format: %v, error: %v", string(body), err)
	}

	if !reflect.DeepEqual(actualReponse, expectedResponse) {
		t.Errorf("expected response to equal %+#v, but was %+#v", expectedResponse, actualReponse)
	}
}
