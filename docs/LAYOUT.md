# Layout Guide

Chronograf's applications are built by layout configurations that can be found in the `canned` directory of the repository or the directory specified in with the `--canned-path` of the binary distribution. This document will cover how to create new layouts or modify existing ones to better fill your environment.

To create a new layout use the `new_apps.sh` script in the `canned` directory. This script takes an argument, which is the name of the layout you want, often this will map to the InfluxDB measurement, but it does not have to. For this example I will be creatiting a layout for the zombocom daemon zombocomd. So first step is the run `new_app.sh zombocomd` this will create a file called `zombocomd.json`. The file will look something like:

```json
 {
    "id": "d77917f4-9305-4c9c-beba-c29bf17a0fd2",
    "measurement": "zombocomd",
    "app": "zombocomd",
    "cells": [{
        "x": 0,
        "y": 0,
        "w": 4,
        "h": 4,
        "i": "b75694f1-5764-4d1d-9d67-564455985894",
        "name": "Running Zombo Average",
        "queries": [{
        "query": "SELECT mean(\"zombo\") FROM zombocomd",
            "db": "telegraf",
            "rp": "",
            "groupbys": ["\"pod\""],
            "wheres": []
        }]
    },
    {
        "x": 0,
        "y": 0,
        "w": 4,
        "h": 4,
        "i": "b75694f1-5764-4d1d-9d67-564455985894",
        "name": "Anythings Per Second",
        "queries": [{
        "query": "SELECT non_negative_derivative(max(\"anything\"), 1s) FROM zombocomd",
            "db": "telegraf",
            "rp": "",
            "groupbys": [],
            "wheres": []
        }]
    }]
 }
```

The meanin of the fields are as follows:
* id - A unique identifier for this file. Because apps can have more than one file, this needs to be unique or your layout will not show up.
* measurement - The name of the [measurement](https://docs.influxdata.com/influxdb/v1.1/concepts/glossary/#measurement) to search. This is used to indicate to chronograf whether the application should be listed for a host.
* app - The name of the application, this does not need to be unique. All layouts for an app will be displayed when the layout is requested.
* cell - An array of graphs
* x - Not currently used
* y - Not currently used
* w - The width of the graph
* h - The height of the graph
* i - A unique ID for the graph
* name - The displayed name of the graph
* queries - An array of InfluxQL queries
* db - The name of the database for the query
* rp - The [retention policy](https://docs.influxdata.com/influxdb/v1.1/concepts/glossary/#retention-policy-rp) for the database
* groupbys - An array of GROUP BY claues to use on the query
* wheres - An array of WHERE clauses to add to the query

The above example will create two graphs. The first graph will show the mean number of zombos created then group the zombos by pod and by time. It is important to note that all queries have a `GROUP BY` time(x) appended to them. In this case `x` is determined by the duration of the query. The second graph will show the number of the "anything" [field](https://docs.influxdata.com/influxdb/v1.1/concepts/glossary/#field) per second average.
For real world examples of when to use the `groupbys` see the docker examples and for `wheres` examples see the kubernetes examples in the `canned` directory.
