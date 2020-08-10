# HTTP Response Input Plugin

This input plugin checks HTTP/HTTPS connections.

### Configuration:

```
# HTTP/HTTPS request given an address a method and a timeout
[[inputs.http_response]]
  ## Deprecated in 1.12, use 'urls'
  ## Server address (default http://localhost)
  # address = "http://localhost"

  ## List of urls to query.
  # urls = ["http://localhost"]

  ## Set http_proxy (telegraf uses the system wide proxy settings if it's is not set)
  # http_proxy = "http://localhost:8888"

  ## Set response_timeout (default 5 seconds)
  # response_timeout = "5s"

  ## HTTP Request Method
  # method = "GET"

  ## Whether to follow redirects from the server (defaults to false)
  # follow_redirects = false

  ## Optional HTTP Request Body
  # body = '''
  # {'fake':'data'}
  # '''

  ## Optional name of the field that will contain the body of the response. 
  ## By default it is set to an empty String indicating that the body's content won't be added 
  # response_body_field = ''

  ## Maximum allowed HTTP response body size in bytes.
  ## 0 means to use the default of 32MiB.
  ## If the response body size exceeds this limit a "body_read_error" will be raised
  # response_body_max_size = "32MiB"

  ## Optional substring or regex match in body of the response (case sensitive)
  # response_string_match = "\"service_status\": \"up\""
  # response_string_match = "ok"
  # response_string_match = "\".*_status\".?:.?\"up\""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## HTTP Request Headers (all values must be strings)
  # [inputs.http_response.headers]
  #   Host = "github.com"

  ## Optional setting to map response http headers into tags
  ## If the http header is not present on the request, no corresponding tag will be added
  ## If multiple instances of the http header are present, only the first value will be used
  # http_header_tags = {"HTTP_HEADER" = "TAG_NAME"}

  ## Interface to use when dialing an address
  # interface = "eth0"
```

### Metrics:

- http_response
  - tags:
    - server (target URL)
    - method (request method)
    - status_code (response status code)
    - result ([see below](#result--result_code))
  - fields:
    - response_time (float, seconds)
    - content_length (int, response body length)
    - response_string_match (int, 0 = mismatch / body read error, 1 = match)
    - http_response_code (int, response status code)
	- result_type (string, deprecated in 1.6: use `result` tag and `result_code` field)
    - result_code (int, [see below](#result--result_code))

#### `result` / `result_code`

Upon finishing polling the target server, the plugin registers the result of the operation in the `result` tag, and adds a numeric field called `result_code` corresponding with that tag value.

This tag is used to expose network and plugin errors. HTTP errors are considered a successful connection.

|Tag value                |Corresponding field value|Description|
--------------------------|-------------------------|-----------|
|success                  | 0                       |The HTTP request completed, even if the HTTP code represents an error|
|response_string_mismatch | 1                       |The option `response_string_match` was used, and the body of the response didn't match the regex. HTTP errors with content in their body (like 4xx, 5xx) will trigger this error|
|body_read_error          | 2                       |The option `response_string_match` was used, but the plugin wasn't able to read the body of the response. Responses with empty bodies (like 3xx, HEAD, etc) will trigger this error. Or the option `response_body_field` was used and the content of the response body was not a valid utf-8. Or the size of the body of the response exceeded the `response_body_max_size` |
|connection_failed        | 3                       |Catch all for any network error not specifically handled by the plugin|
|timeout                  | 4                       |The plugin timed out while awaiting the HTTP connection to complete|
|dns_error                | 5                       |There was a DNS error while attempting to connect to the host|


### Example Output:

```
http_response,method=GET,result=success,server=http://github.com,status_code=200 content_length=87878i,http_response_code=200i,response_time=0.937655534,result_code=0i,result_type="success" 1565839598000000000
```
