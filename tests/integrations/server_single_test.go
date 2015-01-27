package integrations_test

//import (
//"io/ioutil"
//"net/url"
//"os"
//"testing"

//"github.com/influxdb/influxdb/tests/integrations"
//)

//func TestNewServer(t *testing.T) {
//var err error
//c := integrations.Config{}

//c.BrokerURL = &url.URL{Scheme: "http:", Host: "localhost:9000"}
//c.BrokerDir, err = ioutil.TempDir("", "broker_9000")
//if err != nil {
//t.Fatal(err)
//}
//// Be sure to clean up when we are dong
//defer func() {
//os.Remove(c.BrokerDir)
//}()

//c.ServerURL = &url.URL{Scheme: "http:", Host: "localhost:9000"}
//c.ServerDir, err = ioutil.TempDir("", "data_9000")
//if err != nil {
//t.Fatal(err)
//}
//// Be sure to clean up when we are dong
//defer func() {
//os.Remove(c.ServerDir)
//}()

//s, err := integrations.NewServer(c)
//if err != nil {
//t.Fatalf("Unable to create a new server: %v", err)
//}
//err = s.Start()
//if err != nil {
//t.Fatalf("Unable to start server: %v", err)
//}
//}
