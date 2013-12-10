package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Client struct {
	host     string
	username string
	password string
	database string
}

type ClientConfig struct {
	Host     string
	Username string
	Password string
	Database string
}

var defaults map[string]string

func init() {
	defaults = map[string]string{
		"host":     "localhost:8086",
		"username": "root",
		"password": "root",
		"database": "",
	}
}

func getDefault(value, key string) string {
	if value == "" {
		return defaults[key]
	}
	return value
}

func NewClient(config *ClientConfig) (*Client, error) {
	host := getDefault(config.Host, "host")
	username := getDefault(config.Username, "username")
	passowrd := getDefault(config.Password, "password")
	database := getDefault(config.Database, "database")

	return &Client{host, username, passowrd, database}, nil
}

func (self *Client) getUrl(path string) string {
	return fmt.Sprintf("http://%s%s?u=%s&p=%s", self.host, path, self.username, self.password)
}

func responseToError(response *http.Response, err error, closeResponse bool) error {
	if err != nil {
		return err
	}
	if closeResponse {
		defer response.Body.Close()
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("Server returned (%d): %s", response.StatusCode, string(body))
}

func (self *Client) CreateDatabase(name string) error {
	url := self.getUrl("/db")
	payload := map[string]string{"name": name}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) del(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func (self *Client) DeleteDatabase(name string) error {
	url := self.getUrl("/db/" + name)
	resp, err := self.del(url)
	return responseToError(resp, err, true)
}

func (self *Client) listSomething(url string) ([]map[string]interface{}, error) {
	resp, err := http.Get(url)
	err = responseToError(resp, err, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	somethings := []map[string]interface{}{}
	err = json.Unmarshal(body, &somethings)
	if err != nil {
		return nil, err
	}
	return somethings, nil
}

func (self *Client) GetDatabaseList() ([]map[string]interface{}, error) {
	url := self.getUrl("/db")
	return self.listSomething(url)
}

func (self *Client) CreateClusterAdmin(name, password string) error {
	url := self.getUrl("/cluster_admins")
	payload := map[string]string{"name": name, "password": password}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) UpdateClusterAdmin(name, password string) error {
	url := self.getUrl("/cluster_admins/" + name)
	payload := map[string]string{"password": password}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) DeleteClusterAdmin(name string) error {
	url := self.getUrl("/cluster_admins/" + name)
	resp, err := self.del(url)
	return responseToError(resp, err, true)
}

func (self *Client) GetClusterAdminList() ([]map[string]interface{}, error) {
	url := self.getUrl("/cluster_admins")
	return self.listSomething(url)
}

func (self *Client) CreateDatabaseUser(database, name, password string) error {
	url := self.getUrl("/db/" + database + "/users")
	payload := map[string]string{"name": name, "password": password}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) updateDatabaseUserCommon(database, name string, password *string, isAdmin *bool) error {
	url := self.getUrl("/db/" + database + "/users/" + name)
	payload := map[string]interface{}{}
	if password != nil {
		payload["password"] = *password
	}
	if isAdmin != nil {
		payload["admin"] = *isAdmin
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) UpdateDatabaseUser(database, name, password string) error {
	return self.updateDatabaseUserCommon(database, name, &password, nil)
}

func (self *Client) DeleteDatabaseUser(database, name string) error {
	url := self.getUrl("/db/" + database + "/users/" + name)
	resp, err := self.del(url)
	return responseToError(resp, err, true)
}

func (self *Client) GetDatabaseUserList(database string) ([]map[string]interface{}, error) {
	url := self.getUrl("/db/" + database + "/users")
	return self.listSomething(url)
}

func (self *Client) AlterDatabasePrivilege(database, name string, isAdmin bool) error {
	return self.updateDatabaseUserCommon(database, name, nil, &isAdmin)
}

type Series struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

func (self *Client) WriteSeries(series []*Series) error {
	data, err := json.Marshal(series)
	if err != nil {
		return err
	}
	url := self.getUrl("/db/" + self.database + "/series")
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	return responseToError(resp, err, true)
}

func (self *Client) Query(query string) ([]*Series, error) {
	escapedQuery := url.QueryEscape(query)
	url := self.getUrl("/db/" + self.database + "/series")
	url += "&q=" + escapedQuery
	resp, err := http.Get(url)
	err = responseToError(resp, err, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	series := []*Series{}
	err = json.Unmarshal(data, &series)
	if err != nil {
		return nil, err
	}
	return series, nil
}
