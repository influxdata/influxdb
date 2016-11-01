## TL;DR
* Use kapacitor `vars` JSON structure as serialization format.
* Create chronograf endpoint that generates kapacitor tickscripts for all the different UI options.

### Proposal

1. Use kapacitors `vars` JSON structure as the definition of the alert
2. Create service that generates tickscripts.

Currently, there are several alert "triggers" in the wireframe including:

* threshold
* relative value
* deadman

Also, there are several alert destinations like

* slack
* pagerduty
* victorops

Finally, the type of kapacitor tickscript needs to be specified as either:
* `batch`
* `stream`

The generator would take input like this.

```json
{
    "name": "I'm so triggered",
    "version": "1",
	"trigger": "threshold",
	"alerts": ["slack"],
	"type": "stream",
	"vars": {
		"database": {"type" : "string", "value" : "telegraf" },
		"rp": {"type" : "string", "value" : "autogen" },
		"measurement": {"type" : "string", "value" : "disk" },
		"where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
		"groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
		"field": {"type" : "string", "value" : "used_percent" },
		"crit": {"type" : "lambda", "value" : "\"stat\" > 92.0" },
		"window": {"type" : "duration", "value" : "10s" },
		"file": {"type" : "string", "value" : "/tmp/disk_alert_log.txt" }
	}
}
```

and would produce:

 
 ```json
 {
     "name": "I'm so triggered",
     "version": "1",
     "trigger": "threshold",
     "alerts": ["slack"],
     "type": "stream",
     "script": "...", 
     "vars": {
         "database": {"type" : "string", "value" : "telegraf" },
         "rp": {"type" : "string", "value" : "autogen" },
         "measurement": {"type" : "string", "value" : "disk" },
         "where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
         "groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
         "field": {"type" : "string", "value" : "used_percent" },
         "crit": {"type" : "lambda", "value" : "\"stat\" > 92.0" },
         "window": {"type" : "duration", "value" : "10s" },
         "file": {"type" : "string", "value" : "/tmp/disk_alert_log.txt" }
     }
 }
 ```

The cool thing is that the `script`, `vars`, and `type` field can be used directly when POSTing to `/kapacitor/v1/tasks`.

### kapacitor vars
kapacitor `vars` looks like this:

```json
{
	"field_name" : {
		"value": <VALUE>,
		"type": <TYPE>,
		"description": "my cool comment"
	},
	"another_field" : {
		"value": <VALUE>,
		"type": <TYPE>,
		"description": "can I be a cool comment?"
	}
}

```

The following is a table of valid types and example values.

| Type     | Example Value                    | Description                                                                                             |
| ----     | -------------                    | -----------                                                                                             |
| bool     | true                             | "true" or "false"                                                                                       |
| int      | 42                               | Any integer value                                                                                       |
| float    | 2.5 or 67                        | Any numeric value                                                                                       |
| duration | "1s" or 1000000000               | Any integer value interpretted in nanoseconds or an influxql duration string, (i.e. 10000000000 is 10s) |
| string   | "a string"                       | Any string value                                                                                        |
| regex    | "^abc.*xyz"                      | Any string value that represents a valid Go regular expression https://golang.org/pkg/regexp/           |
| lambda   | "\"value\" > 5"                  | Any string that is a valid TICKscript lambda expression                                                 |
| star     | ""                               | No value is required, a star type var represents the literal `*` in TICKscript (i.e. `.groupBy(*)`)     |
| list     | [{"type": TYPE, "value": VALUE}] | A list of var objects. Currently lists may only contain string or star vars                             |

See the [kapacitor](https://github.com/influxdata/kapacitor/blob/master/client/API.md#vars) var API documentation.

## Tickscripts
 Create kapacitor scripts like this:
 
 This script will take the last 10s of disk info and average the used_percent per host. If the average used_percent is greater than 92% it will crit and log to a file.
 
 ```javascript
 stream
     |from()
         .database('telegraf')
         .retentionPolicy('autogen')
         .measurement('disk')
         .groupBy('host')
     |window()
         .period(10s)
         .every(10s)
     |mean('used_percent')
         .as('stat')
     |alert()
         .id('{{ index .Tags "host"}}/disk_used')
         .message('{{ .ID }}:{{ index .Fields "stat" }}')
         .crit(lambda: "stat" > 92)
         .log('/tmp/disk_alert_log.txt')
```

 
### Variables
 Also, kapacitor has the ability to create variables.  So, good style would be:
 
 ```javascript
 var crit = 92
 var period = 10s
 var every = 10s
 
stream
     |from()
         .database('telegraf')
         .retentionPolicy('autogen')
         .measurement('disk')
         .groupBy('host')
     |window()
         .period(period)
         .every(every)
     |mean('used_percent')
         .as('stat')
     |alert()
         .id('{{ index .Tags "host"}}/disk_used')
         .message('{{ .ID }}:{{ index .Fields "stat" }}')
         .crit(lambda: "stat" > crit)
 		 .log('/tmp/disk_alert_log.txt')
 ```
 
 
When using the kapacitor API, to define this script:

```http

POST /kapacitor/v1/tasks HTTP/1.1

{
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "telegraf", "rp" : "autogen"}],
    "script": "stream | \n from() \n .database('telegraf') \n .retentionPolicy('autogen') \n .measurement('disk') \n .groupBy('host') | \n window() \n .period(period) \n .every(every) | \n mean('used_percent') \n .as('stat') | \n alert() \n .id('{{ index .Tags     \"host\"}}/disk_used') \n .message('{{ .ID }}:{{ index .Fields \"stat\" }}') \n .crit(lambda: \"stat\" > crit) \n .log('/tmp/disk_alert_log.txt') \n ",
    "vars" : {
        "crit": {
            "value": 92,
            "type": "int"
        },
        "period": {
            "value": "10s",
            "type": "duration"
		},
        "every": {
            "value": "10s",
            "type": "duration"
        }
    }
}
```
 
### Templates

 However, kapacitor also has templates.  Templates are used to decouple the data from the script itself. However, the template structure does not allow nodes to be templated.
 
 ```javascript
// Which database to use
var database string
// DB's retention policy
var rp = 'autogen'
// Which measurement to consume
var measurement string
// Optional where filter
var where_filter = lambda: TRUE
// Optional list of group by dimensions
var groups = 'host'
// Which field to process
var field string
// Critical criteria, has access to 'mean' field
var crit lambda
// How much data to window
var window duration
// File for the alert
var file = '/tmp/disk_alert_log.txt'
// ID of the alert
var id = '{{ index .Tags "host"}}/disk_used'
// message of the alert
var message = '{{ .ID }}:{{ index .Fields "stat" }}'
 
stream
    |from()
        .database(database)
        .retentionPolicy(rp)
        .measurement(measurement)
        .where(where_filter)
        .groupBy(groups)
    |window()
        .period(window)
        .every(window)
    |mean(field)
		.as('stat')
    |alert()
        .id(id)
        .message(message)
        .crit(crit)
        .log(file)
 ```
 
In the template above some of the fields are typed and some have values.  However, each `var` field can be defined and/or overridden by a `JSON` blob.  For example:

```json
{
	"database": {"type" : "string", "value" : "telegraf" },
	"rp": {"type" : "string", "value" : "autogen" },
	"measurement": {"type" : "string", "value" : "disk" },
	"where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
	"groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
	"field": {"type" : "string", "value" : "used_percent" },
	"crit": {"type" : "lambda", "value" : "\"stat\" > 92.0" },
	"window": {"type" : "duration", "value" : "10s" },
	"file": {"type" : "string", "value" : "/tmp/disk_alert_log.txt" }
}
```

So, to use templates in kapacitor, one posts the template to [`/kapacitor/v1/templates`](https://docs.influxdata.com/kapacitor/v1.0/api/api/#templates).

This template can have a unique id.  To use the template one just sends the `template-id` and the`JSON` blob as `vars` to the tasks endpoint.  

Doing so separates the responsibility of the template creator from the task creator.
Additionally, it is possible to list all the templates via [`/kapacitor/v1/templates`](https://docs.influxdata.com/kapacitor/v1.0/api/api/#list-templates).  

Finally, each template from the list contains the variables that are required to be set.
