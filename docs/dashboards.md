## Dashboard API

### Visualization Metadata
Currently, we don't know all features we want in the dashboard visualization.  So, we'll hand-wave over that until we are actually creating the dashboards.  We'll keep the codebase agile enough to add in visualization metadata as we need 'em.

### TL; DR 
Here are the objects we are thinking about; dashboards contain layouts which contain explorations.

#### Dashboard
```json
{
	"links": {
		"self": "/chronograf/v1/dashboards/myid"
	},
	"id": "myid",
	"layouts": [
		"/chronograf/v1/layouts/a",
		"/chronograf/v1/layouts/b"
	],
	"name": "This is my dashboard",
	... visualization metadata here ...
}
```

#### Layout
(Notice that we added explorations in the cell object) 
```json
{
	"id": "6dfb4d49-20dc-4157-9018-2b1b1cb75c2d",
	"dashboards" :[
		"/chronograf/v1/dashboards/myid"
	],
	"cells": [
		{
		  "visualization": "graph",
		  "name": "Apache Bytes/Second",
		  "exploration": "/chronograf/v1/explorations/1"
		}
	]
}
```

#### Exploration
This is the same object we currently use for explorations.

```json
{
	"name": "my exploration",
	"data":{}
}

```

## API
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
			"layouts": [
				"/chronograf/v1/layouts/a",
				"/chronograf/v1/layouts/b"
			],
			"name": "This is my dashboard",
		},
		{
			"links": {
				"self": "/chronograf/v1/dashboards/myother"
			},
			"id": "myother",
			"layouts": [
				"/chronograf/v1/layouts/b",
				"/chronograf/v1/layouts/c"
			],
			"name": "This is my other dashboard",
		}
	]
}
```
#### GET /dashboards/myid

Returns an single dashboard object

```json
{
    "links": {
        "self": "/chronograf/v1/dashboards/myid"
    },
    "id": "myid",
    "layouts": [
        "/chronograf/v1/layouts/a",
        "/chronograf/v1/layouts/b"
    ],
    "name": "This is my dashboard",
}
```

#### PUT /dashboards/myid
We want complete updates to the dashboards because we don't know if we can actually incrementally update visualization metadata

This endpoint will respond with the dashboard after update.

#### DELETE 
Removes a dashboard but will not remove the explorations or layouts.  This could lead to orphans.


Should we have a page that lists all the explorations and layouts and associated dashboards?

#### POST
Creating a dashboard from an exploration would need to be a two-step process

1. Create Layout containing exploration
2. Create dashboard including Layout link

Perhaps the creation of a new dashboard could create associated layouts?

### Modifications to /layouts

A layout would have an array of associated dashboards.  This is a many-to-many relationship with the dashboards.

If one edits a layout it could edit the layout for all dashboards. If a layout belongs to multiple dashboards, we could prompt the user to decide if they want to edit one or all.

We will add another filter parameter, `dashboard`, to layouts.

To get the layouts associated with dashboards `mydashboard` and `myotherdashboard`:
```http
GET /chronograf/v1/layouts?dashboard=mydashboard&dashboard=myotherdashboard HTTP/1.1
```



