export const source = {
  id: '16',
  name: 'ssl',
  type: 'influx',
  username: 'admin',
  url: 'https://localhost:9086',
  insecureSkipVerify: true,
  default: false,
  telegraf: 'telegraf',
  organization: '0',
  role: 'viewer',
  links: {
    self: '/chronograf/v1/sources/16',
    kapacitors: '/chronograf/v1/sources/16/kapacitors',
    proxy: '/chronograf/v1/sources/16/proxy',
    queries: '/chronograf/v1/sources/16/queries',
    write: '/chronograf/v1/sources/16/write',
    permissions: '/chronograf/v1/sources/16/permissions',
    users: '/chronograf/v1/sources/16/users',
    databases: '/chronograf/v1/sources/16/dbs',
  },
}

export const query = {
  id: '0',
  database: 'db1',
  measurement: 'm1',
  retentionPolicy: 'r1',
  fill: 'null',
  fields: [
    {
      value: 'f1',
      type: 'field',
      alias: 'foo',
      args: [],
    },
  ],
  tags: {
    tk1: ['tv1', 'tv2'],
  },
  groupBy: {
    time: null,
    tags: [],
  },
  areTagsAccepted: true,
  rawText: null,
  status: null,
  shifts: [],
}

export const kapacitor = {
  url: '/foo/bar/baz',
  name: 'kapa',
  username: 'influx',
  password: '',
  active: false,
  links: {
    self: '/kapa/1',
    proxy: '/proxy/kapacitor/1',
  },
}

export const kapacitorRules = [
  {
    id: 'chronograf-v1-1bb60c5d-9c46-4601-8fdd-930ac5d2ae3d',
    tickscript:
      "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'cpu'\n\nvar groupBy = ['cpu']\n\nvar whereFilter = lambda: (\"cpu\" != 'cpu-total' OR \"cpu\" != 'cpu1')\n\nvar period = 1h\n\nvar name = 'asdfasdfasdfasdfbob'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'deadman'\n\nvar threshold = 0.0\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n\nvar trigger = data\n    |deadman(threshold, period)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n        .stateChangesOnly()\n        .pushover()\n        .pushover()\n        .sensu()\n        .source('Kapacitorsdfasdf')\n        .handlers()\n\ntrigger\n    |eval(lambda: \"emitted\")\n        .as('value')\n        .keep('value', messageField, durationField)\n    |eval(lambda: float(\"value\"))\n        .as('value')\n        .keep()\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
    query: {
      id: 'chronograf-v1-1bb60c5d-9c46-4601-8fdd-930ac5d2ae3d',
      database: 'telegraf',
      measurement: 'cpu',
      retentionPolicy: 'autogen',
      fields: [],
      tags: {
        cpu: ['cpu-total', 'cpu1'],
      },
      groupBy: {
        time: '',
        tags: ['cpu'],
      },
      areTagsAccepted: false,
      rawText: null,
      range: null,
      shifts: null,
    },
    every: '',
    alertNodes: {
      typeOf: 'alert',
      stateChangesOnly: true,
      useFlapping: false,
      post: [],
      tcp: [],
      email: [],
      exec: [],
      log: [],
      victorOps: [],
      pagerDuty: [],
      pushover: [
        {
          userKey: '',
          device: '',
          title: '',
          url: '',
          urlTitle: '',
          sound: '',
        },
        {
          userKey: '',
          device: '',
          title: '',
          url: '',
          urlTitle: '',
          sound: '',
        },
      ],
      sensu: [
        {
          source: 'Kapacitorsdfasdf',
          handlers: [],
        },
      ],
      slack: [],
      telegram: [],
      hipChat: [],
      alerta: [],
      opsGenie: [],
      talk: [],
    },
    message: '',
    details: '',
    trigger: 'deadman',
    values: {
      period: '1h0m0s',
      rangeValue: '',
    },
    name: 'asdfasdfasdfasdfbob',
    type: 'stream',
    dbrps: [
      {
        db: 'telegraf',
        rp: 'autogen',
      },
    ],
    status: 'enabled',
    executing: true,
    error: '',
    created: '2018-01-05T15:40:48.195743458-08:00',
    modified: '2018-03-13T17:17:23.991640555-07:00',
    'last-enabled': '2018-03-13T17:17:23.991640555-07:00',
    links: {
      self:
        '/chronograf/v1/sources/1/kapacitors/1/rules/chronograf-v1-1bb60c5d-9c46-4601-8fdd-930ac5d2ae3d',
      kapacitor:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-1bb60c5d-9c46-4601-8fdd-930ac5d2ae3d',
      output:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-1bb60c5d-9c46-4601-8fdd-930ac5d2ae3d%2Foutput',
    },
  },
]
