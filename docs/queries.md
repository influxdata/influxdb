## 
Query proxy will be a façade over InfluxDB, InfluxDB Enterprise Cluster, and InfluxDB Relay.  

It will provide a uniform interface to `SELECT` a time range of data.

```http
POST /enterprise/v1/sources/{id}/query HTTP/1.1
Accept: application/json
Content-Type: application/json
{
 "query": "SELECT * from telegraf",
 "format": "dygraph",
 "max_points": 1000,
 "type": "http"
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
