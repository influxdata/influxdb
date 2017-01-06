const kapacitor = () => {
  return ({
    "id": "1",
    "name": "kapa",
    "url": "http://chronograf.influxcloud.net:9092",
    "username": "testuser",
    "password": "hunter2",
    "links": {
      "proxy": "http://localhost:3888/chronograf/v1/sources/2/kapacitors/1/proxy",
      "self": "http://localhost:3888/chronograf/v1/sources/2/kapacitors/1",
      "rules": "http://localhost:3888/chronograf/v1/sources/2/kapacitors/1/rules"
    }
  });
}

export default kapacitor;
