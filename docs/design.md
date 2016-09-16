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
3. User administration for Influx Enterprise.
4. Leverage our existing enterprise front-end code.
5. Leverage lessons-learned for enterprise back-end code.

### Versions

Each version will contain more and more features around monitoring various devops components. 

#### Features

1. v1
	- Data explorer for both OSS and Enterprise
	- Dashboards for telegraf system metrics
	- User and Role adminstration
	- Proxy queries over OSS and Enterprise
	- Authenticate against OSS/Enterprise

2. v2
	- Telegraf agent service
	- Additional Dashboards for telegraf agent
	
### Supported Versions of TICK Stack
We will only support 1.0 of the TICK stack.


### Closed source vs Open Source

- Ideally, we would use the soon-to-be open source plutonium client to interact with Influx Enterprise. This would mean that this application could be entirely open source. (We should check with Todd and Nate.)
- However, if in the future we want to deliver a closed source version, we'll use the open source version as a library.  The open source library will define certain routes (/users, /whatever); the closed source version will either override those routes, or add new ones.  This implies that the closed source version is simply additional or manipulated routes on the server.
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

At first, we'll autogenerate the http server in go from the swagger definition.  This will free the team up from implementing the http validation, etc. and work strictly on 
business logic.  Towards the end of the development cycle we'll implement the http routing and JSON validation.

#### Queries

Features would include:

1. Load balancing against all data nodes in cluster.
1. Formatting the output results to be simple to use in frontend.
1. Decimating the results to minimize network traffic.
1. Use parameters to move query time range.
1. Allow different types of response protocols (http GET, websocket, etc.).

- **`/proxy`:** used to send queries directly to the Influx backend.  They should be most useful for the data explorer or other ad hoc query functionality.

##### `/proxy` Queries

Queries to the `/proxy` endpoint do not create new REST resources.  Instead, it returns results of the query.

This endpoint uses POST with a JSON object to specify the query and the parameters.  The endpoint's response will be the results of the query, or, the errors from the backend InfluxDB.

Errors in the 4xx range come from the Influxdb data source.

```sequence
App->/proxy: POST query
Note right of /proxy: Query Validation
Note right of /proxy: Load balance query
/proxy->Influx/Relay/Cluster: SELECT
Influx/Relay/Cluster-->/proxy: Time Series
Note right of /proxy: Format
/proxy-->App: Formatted results
```

Request:

```http
POST /enterprise/v1/sources/{id}/proxy HTTP/1.1
Accept: application/json
Content-Type: application/json

{
  	"query": "SELECT * from telegraf where time > $value",
  	"format": "dygraph",
}
```

Response:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
	"results": "..." 
}
```

Error Response:

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
	"code": 400,
	"message": "error parsing query: found..."
}
```

##### Load balancing

Use simple round robin load balancing requests to data nodes.
Discover active data nodes using Plutonium meta client.

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

1. Dashboards

	- precanned dashboards for telegraf
	- Includes location of query resources.

1. Queries
	- Used to construct influxql.

1. Sessions
	- We could simply use the JWT token as the session information
	
1. Server Configuration
	
	- Any setting that would normally in TICK stack land be in a file, we'll expose through an updatable API.
	- License/Organization info, modules(pre-canned dash, query builder, customer dash, config builder), usage and debug history/info, support contacts

#### Authentication

We want the backend data store (influx oss or influx meta) handle the authentication so that the web server has less responsibility.

We'll use JWT throughout.

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
