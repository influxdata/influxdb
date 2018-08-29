import {
  Source,
  SourceAuthenticationMethod,
  Template,
  Dashboard,
  Cell,
  CellType,
  SourceLinks,
  TemplateType,
  TemplateValueType,
} from 'src/types'

export const queryConfig = {
  queries: [
    {
      id: '60842c85-8bc7-4180-a844-b974e47a98cd',
      query:
        'SELECT mean(:fields:), mean("usage_user") AS "mean_usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)',
      queryConfig: {
        id: '60842c85-8bc7-4180-a844-b974e47a98cd',
        database: 'telegraf',
        measurement: 'cpu',
        retentionPolicy: 'autogen',
        fields: [
          {
            value: 'mean',
            type: 'func',
            alias: '',
            args: [{value: 'usage_idle', type: 'field', alias: ''}],
          },
          {
            value: 'mean',
            type: 'func',
            alias: 'mean_usage_user',
            args: [{value: 'usage_user', type: 'field', alias: ''}],
          },
        ],
        tags: {},
        groupBy: {time: 'auto', tags: []},
        areTagsAccepted: false,
        fill: 'null',
        rawText:
          'SELECT mean(:fields:), mean("usage_user") AS "mean_usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)',
        range: null,
        shifts: [],
      },
      queryTemplated:
        'SELECT mean("usage_idle"), mean("usage_user") AS "mean_usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)',
      tempVars: [
        {
          tempVar: ':fields:',
          values: [{value: 'usage_idle', type: 'fieldKey', selected: true}],
        },
      ],
    },
  ],
}

export const sourceLinks: SourceLinks = {
  query: '/chronograf/v1/sources/16/query',
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
  links: sourceLinks,
  authentication: SourceAuthenticationMethod.Basic,
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
    {
      value: 'us-west',
      type: TemplateValueType.TagKey,
      selected: false,
      localSelected: false,
    },
    {
      value: 'us-east',
      type: TemplateValueType.TagKey,
      selected: true,
      localSelected: true,
    },
    {
      value: 'us-mount',
      type: TemplateValueType.TagKey,
      selected: false,
      localSelected: false,
    },
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
  inView: true,
}
