const query = () => {
  return ({
    "id": "ad64c9e3-11d9-4e1a-bb6f-e80e09aec1cf",
    "database": "telegraf",
    "measurement": "cpu",
    "retentionPolicy": "autogen",
    "fields": [
      {
        "field": "usage_idle",
        "funcs": [
          "mean"
        ]
      }
    ],
    "tags": {},
    "groupBy": {
      "time": "10s",
      "tags": []
    },
    "areTagsAccepted": true
  });
}

export default query;
