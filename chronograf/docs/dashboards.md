## Dashboard API
The dashboard API will support collections of resizable InfluxQL visualizations.

### TL; DR 
Here are the objects we are thinking about; dashboards contain layouts which
contain queries.

#### Dashboard

```json
{
	"links": {
		"self": "/chronograf/v1/dashboards/myid"
	},
	"id": "myid",
	"cells": [
		{
			"x": 0,
			"y": 0,
			"w": 2,
			"h": 1,
			"label": "Objects/Second",
			"range": {
				"upper": 100,
				"lower": 10
			},
			"queries": ["/chronograf/v1/queries/1"]
		},
		{
			"x": 2,
			"y": 0,
			"w": 2,
			"h": 1,
			"label": "Widgets/Second",
			"range": {
				"upper": 10,
				"lower": 0
			},
			"queries": ["/chronograf/v1/queries/2"]
		}
	],
	"name": "This is my dashboard",
}
```

#### Query

```json
{
	"query": "SELECT mean(n_cpu) from cpu GROUP BY time(15m)",
	"db": "telegraf",
	"rp": "",
	"links" : {
		"self": "/chronograf/v1/queries/1"
	}
}

```
## Older Chronograf dashboards
Older chronograf dashboards had the following features:

* View Dashboard
* Add new visualization to dashboard
* Add existing visualization to dashboard
* Remove visualizatoin from dashboard
* Naming of dashboards
* Delete dashboard
* Edit visualization in dashboard

## API
This API supports a user defined grid layout by specifying `x`, `y`, `w`, `h`.
These fields align with the [react-grid-layout](https://github.com/STRML/react-grid-layout#usage)
methodology.

This layout style has:

* Draggable query cells
* Resizable query cells
* Vertical auto-packing

### /dashboards
#### GET /dashboards

Returns a list of dashboards

```json
{
    "dashboards": [
        {
            "links": {
                "self": "/chronograf/v1/dashboards/myid"
            },
            "id": "myid",
            "cells": [
                {
                    "x": 0,
                    "y": 0,
                    "w": 2,
                    "h": 1,
                    "label": "Objects/Second",
                    "range": {
                        "upper": 100,
                        "lower": 10
                    },
                    "queries": ["/chronograf/v1/queries/1"]
                },
                {
                    "x": 2,
                    "y": 0,
                    "w": 2,
                    "h": 1,
                    "label": "Widgets/Second",
                    "range": {
                        "upper": 10,
                        "lower": 0
                    },
                    "queries": ["/chronograf/v1/queries/2"]
                }
            ],
            "name": "This is my dashboard"
        },
        {
            "links": {
                "self": "/chronograf/v1/dashboards/myid"
            },
            "id": "myid",
            "cells": [
                {
                    "x": 0,
                    "y": 0,
                    "w": 2,
                    "h": 1,
                    "label": "Objects/Second",
                    "range": {
                        "upper": 100,
                        "lower": 10
                    },
                    "queries": ["/chronograf/v1/queries/1"]
                },
                {
                    "x": 2,
                    "y": 0,
                    "w": 2,
                    "h": 1,
                    "label": "Widgets/Second",
                    "range": {
                        "upper": 10,
                        "lower": 0
                    },
                    "queries": ["/chronograf/v1/queries/2"]
                }
            ],
            "name": "This is my dashboard"
        }
    ]
}
```
#### GET /dashboards/myid

Returns an single dashboard object.  The fields `x`, `y`, `w`, `h`, default to 
not existing unless a user has modified the particular cell.

```json
{
    "links": {
        "self": "/chronograf/v1/dashboards/myid"
    },
    "id": "myid",
    "cells": [
        {
            "x": 0,
            "y": 0,
            "w": 2,
            "h": 1,
            "label": "Objects/Second",
            "range": {
                "upper": 100,
                "lower": 10
            },
            "queries": ["/chronograf/v1/queries/1"]
        },
        {
            "x": 2,
            "y": 0,
            "w": 2,
            "h": 1,
            "label": "Widgets/Second",
            "range": {
                "upper": 10,
                "lower": 0
            },
            "queries": ["/chronograf/v1/queries/2"]
        }
    ],
    "name": "This is my dashboard"
}
```

#### PUT /dashboards/myid
We want complete updates to the dashboards because we don't know if we can 
actually incrementally update visualization metadata

This endpoint will respond with the dashboard after update.

#### DELETE 
Removes a dashboard but will not remove the queries.  This will lead to orphans.

Should we have a page that lists all the queries?

#### POST
Creating a dashboard from an exploration would need to be a two-step process

1. Create a query
2. Create dashboard including query link

### /queries
The queries endpoint will be a simple CRUD endpoint storing the raw query:

```json
{
	"query": "SELECT mean(n_cpu) from cpu GROUP BY time(15m)",
	"db": "telegraf",
	"rp": "",
	"links" : {
		"self": "/chronograf/v1/queries/1"
	}
}
```

Over time I'd like this queries endpoint to support a `format` parameter to return 
the query data in different JSON formats such as `QueryConfig`.
