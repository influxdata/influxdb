const rule = ({
  trigger,
  values,
}) => {
  values = {
    "rangeOperator": "greater than",
    "change": "change",
    "operator": "greater than",
    "shift": "1m",
    "value": "10",
    "rangeValue": "20",
    "period": "10m",
    ...values,
  };

  return ({
    "id": "chronograf-v1-08cdb16b-7874-4c8f-858d-1c07043cb2f5",
    "query": {
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
    },
    "every": "30s",
    "alerts": [],
    "message": "",
    trigger,
    values,
    "name": "Untitled Rule",
    "tickscript": "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'cpu'\n\nvar groupBy = []\n\nvar whereFilter = lambda: TRUE\n\nvar period = 10s\n\nvar every = 30s\n\nvar name = 'Untitled Rule'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'threshold'\n\nvar lower = 10\n\nvar upper = 20\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n    |window()\n        .period(period)\n        .every(every)\n        .align()\n    |mean('usage_idle')\n        .as('value')\n\nvar trigger = data\n    |alert()\n        .crit(lambda: \"value\" < lower AND \"value\" > upper)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n\ntrigger\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
    "links": {
      "self": "/chronograf/v1/sources/2/kapacitors/1/rules/chronograf-v1-08cdb16b-7874-4c8f-858d-1c07043cb2f5",
      "kapacitor": "/chronograf/v1/sources/2/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-08cdb16b-7874-4c8f-858d-1c07043cb2f5",
      "output": "/chronograf/v1/sources/2/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-08cdb16b-7874-4c8f-858d-1c07043cb2f5%2Foutput"
    },
    "queryID": "ad64c9e3-11d9-4e1a-bb6f-e80e09aec1cf"
  });
}

export default rule;
