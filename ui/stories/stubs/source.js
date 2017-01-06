const source = () => {
  return ({
    "id": "2",
    "name": "test-user",
    "username": "test-user",
    "password": "hunter2",
    "url": "http://chronograf.influxcloud.net:8086",
    "default": true,
    "telegraf": "telegraf",
    "links": {
      "self": "http://localhost:3888/chronograf/v1/sources/2",
      "kapacitors": "http://localhost:3888/chronograf/v1/sources/2/kapacitors",
      "proxy": "http://localhost:3888/chronograf/v1/sources/2/proxy"
    }
  })
}

export default source;
