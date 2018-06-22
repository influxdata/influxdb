import {Source, Template, Dashboard, Cell, CellType} from 'src/types'
import {SourceLinks, TemplateType, TemplateValueType} from 'src/types'

export const role = {
  name: '',
  organization: '',
}

export const currentOrganization = {
  name: '',
  defaultRole: '',
  id: '',
  links: {
    self: '',
  },
}

export const me = {
  currentOrganization,
  role,
}

export const sourceLinks: SourceLinks = {
  services: '/chronograf/v1/sources/16/services',
  self: '/chronograf/v1/sources/16',
  kapacitors: '/chronograf/v1/sources/16/kapacitors',
  proxy: '/chronograf/v1/sources/16/proxy',
  queries: '/chronograf/v1/sources/16/queries',
  write: '/chronograf/v1/sources/16/write',
  permissions: '/chronograf/v1/sources/16/permissions',
  users: '/chronograf/v1/sources/16/users',
  databases: '/chronograf/v1/sources/16/dbs',
  annotations: '/chronograf/v1/sources/16/annotations',
  health: '/chronograf/v1/sources/16/health',
}

export const source: Source = {
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
  defaultRP: '',
  links: sourceLinks,
}

export const timeRange = {
  lower: 'now() - 15m',
  upper: null,
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
  insecureSkipVerify: false,
  links: {
    self: '/kapa/1',
    proxy: '/proxy/kapacitor/1',
  },
}

export const service = {
  id: '1',
  sourceID: '1',
  url: 'localhost:8082',
  type: 'flux',
  name: 'Flux',
  username: '',
  password: '',
  active: false,
  insecureSkipVerify: false,
  links: {
    source: '/chronograf/v1/sources/1',
    proxy: '/chronograf/v1/sources/1/services/2/proxy',
    self: '/chronograf/v1/sources/1/services/2',
  },
  metadata: {},
}

export const kapacitorRules = [
  {
    id: '1',
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
  {
    id: 'chronograf-v1-75b638b0-1530-4163-adab-c9631386e0a2',
    tickscript:
      "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'disk'\n\nvar groupBy = []\n\nvar whereFilter = lambda: TRUE\n\nvar name = 'Untitled bob'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'threshold'\n\nvar crit = 0\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n    |eval(lambda: \"inodes_free\")\n        .as('value')\n\nvar trigger = data\n    |alert()\n        .crit(lambda: \"value\" == crit)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n        .stateChangesOnly()\n        .email()\n        .pagerDuty()\n        .alerta()\n        .environment('bob')\n        .origin('kapacitoadfr')\n        .services()\n\ntrigger\n    |eval(lambda: float(\"value\"))\n        .as('value')\n        .keep()\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
    query: {
      id: 'chronograf-v1-75b638b0-1530-4163-adab-c9631386e0a2',
      database: 'telegraf',
      measurement: 'disk',
      retentionPolicy: 'autogen',
      fields: [
        {
          value: 'inodes_free',
          type: 'field',
          alias: '',
        },
      ],
      tags: {},
      groupBy: {
        time: '',
        tags: [],
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
      email: [
        {
          to: [],
        },
      ],
      exec: [],
      log: [],
      victorOps: [],
      pagerDuty: [
        {
          serviceKey: '',
        },
      ],
      pushover: [],
      sensu: [],
      slack: [],
      telegram: [],
      hipChat: [],
      alerta: [
        {
          token: '',
          resource: '',
          event: '',
          environment: 'bob',
          group: '',
          value: '',
          origin: 'kapacitoadfr',
          service: [],
        },
      ],
      opsGenie: [],
      talk: [],
    },
    message: '',
    details: '',
    trigger: 'threshold',
    values: {
      operator: 'equal to',
      value: '0',
      rangeValue: '',
    },
    name: 'Untitled bob',
    type: 'stream',
    dbrps: [
      {
        db: 'telegraf',
        rp: 'autogen',
      },
    ],
    status: 'disabled',
    executing: false,
    error: '',
    created: '2018-01-05T15:41:22.759905067-08:00',
    modified: '2018-03-14T18:46:37.940091231-07:00',
    'last-enabled': '2018-03-14T18:46:32.409262103-07:00',
    links: {
      self:
        '/chronograf/v1/sources/1/kapacitors/1/rules/chronograf-v1-75b638b0-1530-4163-adab-c9631386e0a2',
      kapacitor:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-75b638b0-1530-4163-adab-c9631386e0a2',
      output:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-75b638b0-1530-4163-adab-c9631386e0a2%2Foutput',
    },
  },
  {
    id: 'chronograf-v1-7734918d-b8b6-460d-a416-34767ba76faa',
    tickscript:
      "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'cpu'\n\nvar groupBy = []\n\nvar whereFilter = lambda: (\"host\" == 'Bobs-MacBook-Pro.local')\n\nvar period = 24h\n\nvar name = 'xena'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'deadman'\n\nvar threshold = 0.0\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n\nvar trigger = data\n    |deadman(threshold, period)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n        .hipChat()\n        .room('asdf')\n\ntrigger\n    |eval(lambda: \"emitted\")\n        .as('value')\n        .keep('value', messageField, durationField)\n    |eval(lambda: float(\"value\"))\n        .as('value')\n        .keep()\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
    query: {
      id: 'chronograf-v1-7734918d-b8b6-460d-a416-34767ba76faa',
      database: 'telegraf',
      measurement: 'cpu',
      retentionPolicy: 'autogen',
      fields: [],
      tags: {
        host: ['Bobs-MacBook-Pro.local'],
      },
      groupBy: {
        time: '',
        tags: [],
      },
      areTagsAccepted: true,
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
      pushover: [],
      sensu: [],
      slack: [],
      telegram: [],
      hipChat: [
        {
          room: 'asdf',
          token: '',
        },
      ],
      alerta: [],
      opsGenie: [],
      talk: [],
    },
    message: '',
    details: '',
    trigger: 'deadman',
    values: {
      period: '24h0m0s',
      rangeValue: '',
    },
    name: 'xena',
    type: 'stream',
    dbrps: [
      {
        db: 'telegraf',
        rp: 'autogen',
      },
    ],
    status: 'disabled',
    executing: false,
    error: '',
    created: '2018-01-05T15:44:54.657212781-08:00',
    modified: '2018-03-13T17:17:19.099800735-07:00',
    'last-enabled': '2018-03-13T17:17:15.964357573-07:00',
    links: {
      self:
        '/chronograf/v1/sources/1/kapacitors/1/rules/chronograf-v1-7734918d-b8b6-460d-a416-34767ba76faa',
      kapacitor:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-7734918d-b8b6-460d-a416-34767ba76faa',
      output:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-7734918d-b8b6-460d-a416-34767ba76faa%2Foutput',
    },
  },
  {
    // if rule has no `query` key, it will display as a tickscript task only
    id: 'chronograf-v1-7734918d-b8b6-460d-a416-34767ba76aac',
    tickscript:
      "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'cpu'\n\nvar groupBy = []\n\nvar whereFilter = lambda: (\"host\" == 'Bobs-MacBook-Pro.local')\n\nvar period = 24h\n\nvar name = 'xena'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'deadman'\n\nvar threshold = 0.0\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n\nvar trigger = data\n    |deadman(threshold, period)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n        .hipChat()\n        .room('asdf')\n\ntrigger\n    |eval(lambda: \"emitted\")\n        .as('value')\n        .keep('value', messageField, durationField)\n    |eval(lambda: float(\"value\"))\n        .as('value')\n        .keep()\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
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
      pushover: [],
      sensu: [],
      slack: [],
      telegram: [],
      hipChat: [
        {
          room: 'asdf',
          token: '',
        },
      ],
      alerta: [],
      opsGenie: [],
      talk: [],
    },
    message: '',
    details: '',
    trigger: 'deadman',
    values: {
      period: '24h0m0s',
      rangeValue: '',
    },
    name: 'pineapples',
    type: 'stream',
    dbrps: [
      {
        db: 'telegraf',
        rp: 'autogen',
      },
    ],
    status: 'enabled',
    executing: false,
    error: '',
    created: '2018-01-05T15:44:54.657212781-08:00',
    modified: '2018-03-13T17:17:19.099800735-07:00',
    'last-enabled': '2018-03-13T17:17:15.964357573-07:00',
    links: {
      self:
        '/chronograf/v1/sources/1/kapacitors/1/rules/chronograf-v1-7734918d-b8b6-460d-a416-34767ba76aac',
      kapacitor:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-7734918d-b8b6-460d-a416-34767ba76aac',
      output:
        '/chronograf/v1/sources/1/kapacitors/1/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-7734918d-b8b6-460d-a416-34767ba76aac%2Foutput',
    },
  },
]

export const authLinks = {
  allUsers: '/chronograf/v1/users',
  auth: [
    {
      callback: '/oauth/github/callback',
      label: 'Github',
      login: '/oauth/github/login',
      logout: '/oauth/github/logout',
      name: 'github',
    },
  ],
  config: {
    auth: '/chronograf/v1/config/auth',
    self: '/chronograf/v1/config',
  },
  dashboards: '/chronograf/v1/dashboards',
  environment: '/chronograf/v1/env',
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  layouts: '/chronograf/v1/layouts',
  logout: '/oauth/logout',
  mappings: '/chronograf/v1/mappings',
  me: '/chronograf/v1/me',
  organizations: '/chronograf/v1/organizations',
  sources: '/chronograf/v1/sources',
  users: '/chronograf/v1/organizations/default/users',
}

export const layout = {
  id: '6dfb4d49-20dc-4157-9018-2b1b1cb75c2d',
  app: 'apache',
  measurement: 'apache',
  autoflow: false,
  cells: [
    {
      x: 0,
      y: 0,
      w: 4,
      h: 4,
      i: '0246e457-916b-43e3-be99-211c4cbc03e8',
      name: 'Apache Bytes/Second',
      queries: [
        {
          query:
            'SELECT non_negative_derivative(max("BytesPerSec")) AS "bytes_per_sec" FROM ":db:".":rp:"."apache"',
          groupbys: ['"server"'],
          label: 'bytes/s',
        },
      ],
      axes: {
        x: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y2: {
          bounds: [],
          label: '',
          prefix: '',
          suffix: '',
          base: '',
          scale: '',
        },
      },
      type: '',
      colors: [],
    },
    {
      x: 4,
      y: 0,
      w: 4,
      h: 4,
      i: '37f2e4bb-9fa5-4891-a424-9df5ce7458bb',
      name: 'Apache - Requests/Second',
      queries: [
        {
          query:
            'SELECT non_negative_derivative(max("ReqPerSec")) AS "req_per_sec" FROM ":db:".":rp:"."apache"',
          groupbys: ['"server"'],
          label: 'requests/s',
        },
      ],
      axes: {
        x: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y2: {
          bounds: [],
          label: '',
          prefix: '',
          suffix: '',
          base: '',
          scale: '',
        },
      },
      type: '',
      colors: [],
    },
    {
      x: 8,
      y: 0,
      w: 4,
      h: 4,
      i: 'ea9174b3-2b56-4e80-a37d-064507c6775a',
      name: 'Apache - Total Accesses',
      queries: [
        {
          query:
            'SELECT non_negative_derivative(max("TotalAccesses")) AS "tot_access" FROM ":db:".":rp:"."apache"',
          groupbys: ['"server"'],
          label: 'accesses/s',
        },
      ],
      axes: {
        x: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y: {bounds: [], label: '', prefix: '', suffix: '', base: '', scale: ''},
        y2: {
          bounds: [],
          label: '',
          prefix: '',
          suffix: '',
          base: '',
          scale: '',
        },
      },
      type: '',
      colors: [],
    },
  ],
  link: {
    href: '/chronograf/v1/layouts/6dfb4d49-20dc-4157-9018-2b1b1cb75c2d',
    rel: 'self',
  },
}

export const hosts = {
  'MacBook-Pro.local': {
    name: 'MacBook-Pro.local',
    deltaUptime: -1,
    cpu: 0,
    load: 0,
  },
}

// Dashboards
export const template: Template = {
  id: '1',
  type: TemplateType.TagKeys,
  label: 'test query',
  tempVar: ':region:',
  query: {
    db: 'db1',
    rp: 'rp1',
    tagKey: 'tk1',
    fieldKey: 'fk1',
    measurement: 'm1',
    influxql: 'SHOW TAGS WHERE CHRONOGIRAFFE = "friend"',
  },
  values: [
    {value: 'us-west', type: TemplateValueType.TagKey, selected: false},
    {value: 'us-east', type: TemplateValueType.TagKey, selected: true},
    {value: 'us-mount', type: TemplateValueType.TagKey, selected: false},
  ],
}

export const dashboard: Dashboard = {
  id: 1,
  cells: [],
  name: 'd1',
  templates: [],
  organization: 'thebestorg',
}

export const cell: Cell = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  i: '0246e457-916b-43e3-be99-211c4cbc03e8',
  name: 'Apache Bytes/Second',
  queries: [],
  axes: {
    x: {
      bounds: ['', ''],
      label: '',
      prefix: '',
      suffix: '',
      base: '',
      scale: '',
    },
    y: {
      bounds: ['', ''],
      label: '',
      prefix: '',
      suffix: '',
      base: '',
      scale: '',
    },
  },
  type: CellType.Line,
  colors: [],
  tableOptions: {
    verticalTimeAxis: true,
    sortBy: {
      internalName: '',
      displayName: '',
      visible: true,
    },
    fixFirstColumn: true,
  },
  fieldOptions: [],
  timeFormat: '',
  decimalPlaces: {
    isEnforced: false,
    digits: 1,
  },
  links: {
    self:
      '/chronograf/v1/dashboards/10/cells/8b3b7897-49b1-422c-9443-e9b778bcbf12',
  },
  legend: {},
}
