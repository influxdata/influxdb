#Influxdb Go Client
>WARNING: The client api will be changing significantly for version 0.9 the example may not exactly represent the current usage. However we will try to keep this readme up2date while the api is changing.

This pkg contains a simple client written in go for influxdb. You can review the godocs [here](http://godoc.org/github.com/influxdb/influxdb/client). We have created afew examples to help you get started.

####A note on imports
I like to use a named import since many things have can be called `client` these days. So first import the influxdb client as follows
```
import (
flux "github.com/influxdb/influxdb/client"
)
```

###Connecting to influxdb
Now you can easily connect to influxdb. Note please make sure the database you specify has been created already.
```
	fluxClient, err := flux.NewClient(&flux.ClientConfig{
		Host:     "localhost:8086",
		Username: "root",
		Password: "root",
		// Note you must create the db
		Database: "example",
	})
	if err != nil {
		log.Fatalln("Error connecting to influxdb:", err)
	}
```

###Writing your first series
Everything in influxdb is know as a `series` it has a name, list of columns, and points. The only slightly hard part is specifing points since they have a slightly uncommon signature of `Points [][]interface{}`. However it's easy once you get the hang of it.
```
	err := fluxClient.WriteSeries([]*flux.Series{
		&flux.Series{
			Name:    "app.testdata",
			Columns: []string{"numberOfGophers", "presentsPerGopher"},
			Points: [][]interface{}{
				{6, 4},
			},
		},
	})
	if err != nil {
		log.Println("Error writing series:", err)
	}
```
Now you should be able to confirm the above data by executing the following query `select * from app.testdata` in the admin dashboard. For more info about influxdb's query language look [here](http://influxdb.com/docs/v0.7/api/query_language.html) it's very similar to sql but please note it's differences. Now you should be all set just remember to match your columns up with your points correctly.


##Where to go from here?
As a fun exercise you can take alook at the following links. To get some more expeirence with influxdb and writing different seiries you can try sending memory/swap/cpu metrics to influxdb and graphing them in grafana.
- [Grafana](http://grafana.org/) - an open source dashboard which has builtin support for influxdb
- [Gosigar](https://github.com/cloudfoundry/gosigar) - A  go/cgo implementation of [sigar](https://github.com/hyperic/sigar)(useful memory/swap/cpu/disk usage)