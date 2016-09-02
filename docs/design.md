Design

### Design Philosophy
	1. Present uniform interface to front-end covering Plutonium and InfluxDB OSS offerings.
	2. Simplify the front-end interaction with time-series database.
	3. Ease of setup and use. 
	4. Extensible as base for future applications
	5. There will be an open source version of this.
	7. Stress Parallel Development across all teams.
	8. Support of on-prem is first class.
	
### Goals
	1. Version 1.1: Produce pre-canned graphs for devops telegraf data for docker containers.
	2. Up and running in 2 minutes
	3. Version 1.1: User administration for OSS and Plutonium
	4. Leverage our existing enterprise front-end code.
	5. Leverage lessons-learned for enterprise back-end code.
	6. Minimum viable product by Oct 10th.
	7. Three to four weeks of testing and polishing before release.

### Version Features:

1.1

1.2

1.3

### Supported Versions of Tick Stack
... what versions are we supporting and not supporting? Nothing pre-1.0?

### Query Proxy
1. Which client do we use? 

	- Current clients are not flexible
	- New client in design phase
	- non-SELECT queries need either Plutonium client or Influx oss client

```sequence
App->Proxy: POST query
Note right of Proxy: Query Validation
Proxy-->App: Location of query resource
App->Proxy: GET Location
Note right of Proxy: Load balance query
Proxy->Influx/Relay/Cluster: SELECT
Note right of Influx/Relay/Cluster: Prepared Telegraf query
Influx/Relay/Cluster-->Proxy: Time Series
Note right of Proxy: Format and Decimate
Proxy-->App: 
Note left of App: Format to dygraph
```

Example: 
```http
POST /enterprise/v1/sources/{id}/query HTTP/1.1
Accept: application/json
Content-Type: application/json
{
 "query": "SELECT * from telegraf where time > $value", // bind parameters
 "format": "dygraph",
 "max_points": 1000,
 "type": "http",
 "ttl": "6h",
 "every": "15s", // possible?
}
```
 Response:
 
 ```http
 HTTP/1.1 202 OK
 {
 	"link": {
 		"rel": "self",
 		"href": "/enterprise/v1/sources/{id}/query/{qid}",
 		"type": "http"
 	}
 }
 ```
 
 #### Websocket sketch?
 We'll avoid websockets for version 1.1 but allow it in the future through the `type` parameter.
 
 c:begin
 s:data
 s:end
 
 c:ping
 s:refresh
 c:accept
 s:data
 s:end
 c:disconnect
 
 c:ping
 c:update
 
### Closed source vs Open Source

- Ideally, we would use the soon-to-be open source plutonium client to interact with Influx Enterprise. This would mean that this application could be entirely open source. (We should check with Todd and Nate.)
- However, if in the future we want to deliever a closed source version, we'll use the open source version as a library.  The open source library will define certain routes (/users /whatever); the closed source version will either override those routes, or add new ones.  This implies that the closed source version is simply additional or manipulated routes on the server.
- Survey the experience of closed source with Jason and Nathaniel.

### Builds
Javascript build will be decoupled from Go build process. 

Asset compilation will happen during build of backend-server.

This allows the front-end team to swap in mocked, auto-generated swagger backend for testing and development.

#### Javascript
Webpack
Static asset compilation during backend-server build.


#### Go

We'll use GDM as the vendoring solution to maintain consistency with other pieces of TICK stack.

*Future work*: we must switch to the community vendoring solution when it actually seems mature.


### Authentication
Do we want shared secret authentication between the server and the influx data store?

How will users be authenticated to the web server?

What we want is to have the backend data store (influx oss or influx meta) handle the authentication so that the web server has less responsibility.

### Backend-server store 
We will build a interface for storing API resources.

Some API resources could come from the influx data source (like users) most will be stored in a key/value or relational store.

Version 1.1 will use boltdb as the key/value store.

Future versions will support more HA data stores.

#### Objects
1. Server Configuration
	
	- Any setting that would normally in TICK stack land be in a file, we'll expose through an updatable API.
	- License/Organization info, modules(pre-canned dash, query builder, customer dash, config builder), usage and debug history/info, support contacts

1. Data source

	- Version 1.1 will have only one data source.
	- InfluxDB
	- InfluxDB Enterprise (this means clustering)
	- InfluxDB relay possibly.
	- Will provide meta data describing the data source (e.g. number of nodes)

1. User

	- Version 1.1 will be a one-to-one mapping to influx.
	- Do we want to extend this user object at some point?  What other properties or relations are important?

1. Layouts

	- We need to have another discussion about the goals.
	- For now the design is an opaque JSON blob until we know how to structure this.

1. Dashboards

	- precanned

1. Sessions for particular user?
	- What data do we persist about a user's session, if any?

### API

We'll use swagger interface definition to specify API and JSON validation.  The goal is to emphasize designing to an interface facilitating parallel development.

### Testing
Talk with Mark and Michael and talk about larger efforts.  This will impact the repository layout.
There is a potentially large testing matrix of components.

#### Integration Testing

- Stress testing.
	- Benchmark pathological queries
- End to end testing. Telegraf -> Plutonium -> Chronograf = expected graph.
- Would be nice to translate user stories to integration tests.
- If someone finds a bug in the integration we need a test so it will never happen again.
- Upgrade testing.
- Version support.

#### Usability and Experience Testing

1. Owned by design team.
1. We are trying to attract the devops crowd.

	- Deployment experience
	- Ease of use.
	- Speed to accomplish task, e.g. find specific info, change setting.
### Collection Agent
Talk to Cameron about distribution/service

Get his opinions on our basic designs (env vars?)

confd vs something built into telegraf

Telegraf authentication jwt?

Prebuilt package (rpm, deb) vs something else.  Are there different config files for each one of these packages?

First implementation similar to Datadog https://app.datadoghq.com/account/settings#agent?
Add environment variables for `INFLUX_URL` and `INFLUX_SHARED_SECRET`. Anything else?

what product order are we supporting? 
1. Docker

Multiple telegrafs to support other services? E.g. a telegraf instance with only Postgres plugin

### User Stories
#### Initial Setup
1. User clicks on an icon that represents their system (e.g. Redhat).
2. User fills out a form that includes the information needed to configure telegraf.
	- influx url
	- influx authentication
	- does telegraf have shared secret jwt?
	- let's talk to nathaniel about this... in regards to how it worked with kapacitor.
3. User gets a download command (sh?)  This command has enough to start a telegraf for docker container monitoring (v1.1) and send the data to influx.

	- Question: how do we remove machines? (e.g. I don't want to see my testing mac laptop anymore)
		- Could use retention policies (fast)
			- testing rp
			- production rp
		- Could use namespaced databases
		- We should talk to the people that working on the new series index to help us handle paging-off of old/inactive instances problem gracefully. DROP SERIES WHERE "host" = 'machine1'

	1. SHOW SERIES WHERE time > 1w # gets all host names for the last week SHOW TAG VALUE WITH KEY = 'host' WHERE time > 1w
	2. Performance??
	2. DROP SERIES WHERE "host" = 'machine1'
	3. We could have a machine endpoint allowing GET/DELETE
	4. we want to filter machines by the times whey were active and the times they first showed up

#### Update telegraf configuration on host
confd for telegraf configuration?
survey prometheus service discovery methodology and compare to our telegraf design (stand-alone service or built-in to telegraf)
