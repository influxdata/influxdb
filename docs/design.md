## Chronograf
[TOC]

### Design Philosophy

1. Present uniform interface to front-end covering Plutonium and InfluxDB OSS offerings.
2. Simplify the front-end interaction with time-series database.
3. Ease of setup and use. 
4. Extensible as base for future applications
5. There will be an open source version of this.
7. Stress Parallel Development across all teams.
8. First class support of on-prem.
9. Release to cloud first.
	
### Initial Goals
1. Produce pre-canned graphs for devops telegraf data for docker containers or system stats.
2. Up and running in 2 minutes
3. User administration for OSS and Plutonium
4. Leverage our existing enterprise front-end code.
5. Leverage lessons-learned for enterprise back-end code.
6. Minimum viable product by Oct 10th.
7. Three to four weeks of testing and polishing before release.

### Versions

Each version will contain more and more features around monitoring various devops components. 

#### Cycles
Two month cycles (typically, one month feature/one month polish)

#### Features

1. Nov
	- Data explorer for both OSS and Enterprise
	- Dashboards for telegraf system metrics
	- User and Role adminstration
	- Proxy queries over OSS and Enterprise
	- Authenticate against OSS/Enterprise

2. Jan
	- Telegraf agent service
	- Additional Dashboards for telegraf agent
	
3. Mar
	- The next stuff

### Supported Versions of Tick Stack
... what versions are we supporting and not supporting? Nothing pre-1.0?


### Closed source vs Open Source

- Ideally, we would use the soon-to-be open source plutonium client to interact with Influx Enterprise. This would mean that this application could be entirely open source. (We should check with Todd and Nate.)
- However, if in the future we want to deliever a closed source version, we'll use the open source version as a library.  The open source library will define certain routes (/users, /whatever); the closed source version will either override those routes, or add new ones.  This implies that the closed source version is simply additional or manipulated routes on the server.
- Survey the experience of closed source with Jason and Nathaniel.

### Repository

#### Structure
Both the javascript and go source will be in the same repository.

#### Builds
Javascript build will be decoupled from Go build process. 

Asset compilation will happen during build of backend-server.

This allows the front-end team to swap in mocked, auto-generated swagger backend for testing and development.

##### Javascript
Webpack
Static asset compilation during backend-server build.


##### Go

We'll use GDM as the vendoring solution to maintain consistency with other pieces of TICK stack.

*Future work*: we must switch to the community vendoring solution when it actually seems mature.

### API

#### REST
We'll use swagger interface definition to specify API and JSON validation.  The goal is to emphasize designing to an interface facilitating parallel development.

#### Query Proxy

The query proxy is a special endpoint to query InfluxDB/InfluxEnterprise.

Features would include:

1. Load balancing against all data nodes in cluster.
1. Formatting the output results to be simple to use in frontend.
1. Decimating the results to minimize network traffic.
1. Use prepared queries to move query window and specify dimensions.
1. Allow different types of response protocols (http GET, websocket, etc.).
1. Efficiently handle many queries at once (for a dashboard).
1. Support multiple InfluxQL queries per request.
1. Only support `SELECT` queries. (no explicit validation? happens in plutonium)

Chronograf will take a different approach than InfluxEnterprise 1.0 and Grafana 2 & 3, which use a GET request with parameters.
It provides two endpoints for euphemeral and persistent queries.
Both endpoints accept a POST request with a JSON object containing similar parameters.

##### Euphemeral Queries

Euphemeral queries are transient and unlikely to be requested multiple times.
They should be most useful for the data explorer or other adhoc query functionality.

Uses a POST request to the `/queries` endpoint and returns results in the response.
No resource is created.
Caching may or may not be available, but should not be relied upon.

```sequence
App->Proxy: POST query
Note right of Proxy: Query Validation
Note right of Proxy: Load balance query
Proxy->Influx/Relay/Cluster: SELECT
Influx/Relay/Cluster-->Proxy: Time Series
Note right of Proxy: Format and Decimate
Proxy-->App: Formatted results
```

Example:

```http
POST /enterprise/v1/sources/{id}/query HTTP/1.1
Accept: application/json
Content-Type: application/json
{
  "query": [
    {
      "name": "query1",
      "query": "SELECT * from telegraf where time > $value"
    }
  ],
  "format": "dygraph",
  "max_points": 1000,
  "type": "http"
}
```

Response:

```http
HTTP/1.1 200 OK
{
  "type": "http",
  "format": "dygraph",
  "query": [
    {
      "name": "explorer1",
      "results": "..." 
    }
  ]
}
```

##### Persistent Queries

Persistent queries are create a resource that provides results for frequent queries.
They should be most useful for dashboards where many queries are needed with similar dimensions.

Uses a POST request to the `/series` endpoint which returns the location of the resource in the response.
Using a GET request will return results for the original queries.
Parameters for prepared statements can be passed in through the GET request.

```sequence
App->Proxy: POST query
Note right of Proxy: Query Validation
Proxy-->App: Location of query resource
App->Proxy: GET Location
Note right of Proxy: Load balance query
Proxy->Influx/Relay/Cluster: SELECT
Influx/Relay/Cluster-->Proxy: Time Series
Note right of Proxy: Format and Decimate
Proxy-->App: 
Note left of App: Format to dygraph
```

Example POST Request:

```http
POST /enterprise/v1/sources/{id}/series HTTP/1.1
Accept: application/json
Content-Type: application/json
{
  "query": [
    {
      "name": "query1",
      "query": "SELECT * from telegraf where time > $value"
    }
  ],
  "format": "dygraph",
  "max_points": 1000,
  "type": "http"
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
		"href": "/enterprise/v1/sources/{id}/series/{qid}",
		"type": "http"
	}
}
```

Example GET Request:

```http
GET /enterprise/v1/sources/{id}/series/{qid} HTTP/1.1
Accept: application/json
Content-Type: application/json
{
  "query": [
    {
      "name": "query1",
      "query": "SELECT * from telegraf where time > $value"
    }
  ],
  "format": "dygraph",
  "max_points": 1000,
  "type": "http"
  "ttl": "6h",
  "every": "15s", // possible?
}
```

Response:
 
```http
HTTP/1.1 200 OK
{
  "type": "http",
  "format": "dygraph",
  "query": [
    {
      "name": "explorer1",
      "results": "..." 
    }
  ]
}
```

##### Websockets

__Not a priority for v1.1__

A websocket protocol will be developed to allow:

	1. dynamic parameter changes
	2. streaming new points

Setting the `type` parameter to `ws` will indicate the client wants to initiate a websockets request.
Simple protocol sketch:

```
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
```

##### Load balancing

Use simple round robin load balancing requests to data nodes.
Discover active data nodes using `influxd-ctl show-cluster` on meta.

##### Questions
_For back-end:_
1. Define how load balancer behaves when influx breaks?

	- retry handling and when to return an error to app
	- rate limit to avoid stampede if node is down

_For front-end:_
1. Is it desirable to use InfluxQL prepared statements or construct query from components (server-side or in-app)?
1. Does separating euphemeral and persistent queries make sense? (Perhaps it sucks to have to do a parameterized GET)
1. SHOW/DROP statements on separate endpoints. (`/users`, `/roles`, `/measurements`, `/tags`, `/fields`)

_For core:_
1. Which influx client do we use?

	- Current clients are not flexible
	- New client in design phase
	- non-SELECT queries need either InfluxDB or InfluxEnterprise client

1. How should we handle cacheing?
1. Use websockets to support a subscription model?
1. Best way to discover whether a node is up?

#### Backend-server store
We will build a interface for storing API resources.

Some API resources could come from the influx data source (like users) most will be stored in a key/value or relational store.

Version 1.1 will use boltdb as the key/value store.

Future versions will support more HA data stores.

##### Objects

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
	- precanned dashboards for telegraf

1. Queries
	- We may store the queries object.
	- Downside: when to expire? TTL?
	- Upside: single endpoint for specific query with bindable parameters.

1. Sessions for particular user?
	- What data do we persist about a user's session, if any?
	
1. Server Configuration
	
	- Any setting that would normally in TICK stack land be in a file, we'll expose through an updatable API.
	- License/Organization info, modules(pre-canned dash, query builder, customer dash, config builder), usage and debug history/info, support contacts

#### Authentication

We want the backend data store (influx oss or influx meta) handle the authentication so that the web server has less responsibility.

##### Questions
1. Do we want shared secret authentication between the server and the influx data store?

2. How will users be authenticated to the web server?


### Testing
Talk with Mark and Michael and talk about larger efforts.  This will impact the repository layout.
There is a potentially large testing matrix of components.

#### Integration Testing
Because we are pulling together so many TICK stack components we will need strong integration testing.

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

The collection agent is at the very least telegraf and a configuration.

The collection agent post-version 1 will feel similar to [Datadog](https://app.datadoghq.com/account/settings#agent).

Good user experience is the key

#### Quesions
1. Talk to Cameron about distribution/service

1. Get his opinions on our basic designs (env vars?)

1. Use confd vs something built into telegraf for dynamic configuration?

1. Telegraf authentication with jwt?

1. Should there be prebuilt package (rpm, deb) vs something else.  Are there different config files for each one of these packages?
1. We could just have environment variables `INFLUX_URL` and `INFLUX_SHARED_SECRET`. Anything else?

1. what product order are we supporting? 
	- v1 system stats
	- v2 Docker stats

1. Multiple telegrafs to support other services? E.g. a telegraf instance with only Postgres plugin

### User Stories
#### Initial Setup v2
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
