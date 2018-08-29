import {interval} from 'src/shared/constants'
import {
  Service,
  Source,
  SourceAuthenticationMethod,
  CellQuery,
  SourceLinks,
  Cell,
  TimeRange,
  Template,
  QueryConfig,
  TemplateType,
  TemplateValueType,
} from 'src/types'
import {
  Axes,
  TableOptions,
  FieldOption,
  DecimalPlaces,
  CellType,
} from 'src/types/dashboards'
import {LineColor, ColorNumber} from 'src/types/colors'

export const dashboard = {
  id: '1',
  name: 'd1',
  cells: [
    {
      x: 1,
      y: 2,
      w: 3,
      h: 4,
      id: '1',
      viewID: '2',
      links: {
        self: '/v2/dashboards/1/cells/1',
        view: '/v2/dashboards/1/cells/1/views',
        copy: '/v2/dashboards/1/cells/1/copy',
      },
    },
  ],
  links: {
    self: '/v2/dashboards/1',
    cells: '/v2/dashboards/cells',
  },
}

export const sourceLinks: SourceLinks = {
  query: '/chronograf/v1/query/4',
  services: '/chronograf/v1/sources/4',
  self: '/chronograf/v1/sources/4',
  kapacitors: '/chronograf/v1/sources/4/kapacitors',
  proxy: '/chronograf/v1/sources/4/proxy',
  queries: '/chronograf/v1/sources/4/queries',
  write: '/chronograf/v1/sources/4/write',
  permissions: '/chronograf/v1/sources/4/permissions',
  users: '/chronograf/v1/sources/4/users',
  databases: '/chronograf/v1/sources/4/dbs',
  annotations: '/chronograf/v1/sources/4/annotations',
  health: '/chronograf/v1/sources/4/health',
}

export const source: Source = {
  id: '4',
  name: 'Influx 1',
  type: 'influx',
  url: 'http://localhost:8086',
  default: false,
  telegraf: 'telegraf',
  links: sourceLinks,
  insecureSkipVerify: false,
  authentication: SourceAuthenticationMethod.Basic,
}

export const service: Service = {
  id: '1',
  sourceID: '1',
  name: 'Flux',
  url: 'http://localhost:8093',
  insecureSkipVerify: false,
  type: 'flux',
  metadata: {
    active: true,
  },
  links: {
    proxy: '/chronograf/v1/sources/1/services/1/proxy',
    self: '/chronograf/v1/sources/1/services/1',
    source: '/chronograf/v1/sources/1',
  },
}

export const queryConfig: QueryConfig = {
  database: 'telegraf',
  measurement: 'cpu',
  retentionPolicy: 'autogen',
  fields: [
    {
      value: 'mean',
      type: 'func',
      alias: 'mean_usage_idle',
      args: [
        {
          value: 'usage_idle',
          type: 'field',
          alias: '',
        },
      ],
    },
    {
      value: 'mean',
      type: 'func',
      alias: 'mean_usage_user',
      args: [
        {
          value: 'usage_user',
          type: 'field',
          alias: '',
        },
      ],
    },
  ],
  tags: {},
  groupBy: {
    time: 'auto',
    tags: [],
  },
  areTagsAccepted: false,
  fill: 'null',
  rawText: null,
  range: null,
  shifts: null,
}

export const query: CellQuery = {
  query:
    'SELECT mean("usage_idle") AS "mean_usage_idle", mean("usage_user") AS "mean_usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)',
  queryConfig,
  source: '',
}

export const axes: Axes = {
  x: {
    bounds: ['', ''],
    label: '',
    prefix: '',
    suffix: '',
    base: '10',
    scale: 'linear',
  },
  y: {
    bounds: ['', ''],
    label: '',
    prefix: '',
    suffix: '',
    base: '10',
    scale: 'linear',
  },
}

export const fieldOptions: FieldOption[] = [
  {
    internalName: 'time',
    displayName: '',
    visible: true,
  },
]

export const tableOptions: TableOptions = {
  verticalTimeAxis: true,
  sortBy: {
    internalName: 'time',
    displayName: '',
    visible: true,
  },
  wrapping: 'truncate',
  fixFirstColumn: true,
}
export const lineColors: LineColor[] = [
  {
    id: '574fb0a3-0a26-44d7-8d71-d4981756acb1',
    type: 'scale',
    hex: '#31C0F6',
    name: 'Nineteen Eighty Four',
    value: '0',
  },
  {
    id: '3b9750f9-d41d-4100-8ee6-bd2785237f35',
    type: 'scale',
    hex: '#A500A5',
    name: 'Nineteen Eighty Four',
    value: '0',
  },
  {
    id: '8d39064f-8124-4967-ae22-ffe14e425781',
    type: 'scale',
    hex: '#FF7E27',
    name: 'Nineteen Eighty Four',
    value: '0',
  },
]

export const decimalPlaces: DecimalPlaces = {
  isEnforced: true,
  digits: 4,
}

export const cell: Cell = {
  i: '67435af2-17bf-4caa-a5fc-0dd1ffb40dab',
  x: 0,
  y: 0,
  w: 8,
  h: 4,
  name: 'Untitled Graph',
  queries: [query],
  axes,
  type: CellType.Line,
  colors: lineColors,
  legend: {},
  tableOptions,
  fieldOptions,
  timeFormat: 'MM/DD/YYYY HH:mm:ss',
  decimalPlaces,
  links: {
    self:
      '/chronograf/v1/dashboards/9/cells/67435af2-17bf-4caa-a5fc-0dd1ffb40dab',
  },
  inView: true,
}

export const fullTimeRange = {
  dashboardID: 9,
  defaultGroupBy: '10s',
  seconds: 300,
  inputValue: 'Past 5 minutes',
  lower: 'now() - 5m',
  upper: null,
  menuOption: 'Past 5 minutes',
  format: 'influxql',
}

export const timeRange: TimeRange = {
  lower: 'now() - 5m',
  upper: null,
}

export const userDefinedTemplateVariables: Template[] = [
  {
    tempVar: ':fields:',
    type: TemplateType.FieldKeys,
    label: '',
    values: [
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_guest',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_guest_nice',
      },
      {
        selected: true,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_idle',
      },
      {
        selected: false,
        localSelected: true,
        type: TemplateValueType.FieldKey,
        value: 'usage_iowait',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_irq',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_nice',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_softirq',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_steal',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_system',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.FieldKey,
        value: 'usage_user',
      },
    ],
    id: '2b8dca84-879c-4555-a7cf-97f2951f8643',
  },
  {
    tempVar: ':measurements:',
    type: TemplateType.Measurements,
    label: '',
    values: [
      {
        selected: true,
        localSelected: true,
        type: TemplateValueType.Measurement,
        value: 'cpu',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'disk',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'diskio',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'mem',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'processes',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'swap',
      },
      {
        selected: false,
        localSelected: false,
        type: TemplateValueType.Measurement,
        value: 'system',
      },
    ],
    id: '18855209-12db-4619-9834-1d7eb643ae6e',
  },
]

const dashtimeTempVar: Template = {
  id: 'dashtime',
  tempVar: ':dashboardTime:',
  type: TemplateType.Constant,
  values: [
    {
      value: 'now() - 5m',
      type: TemplateValueType.Constant,
      selected: true,
      localSelected: true,
    },
  ],
  label: '',
}
const upperdashtimeTempVar: Template = {
  id: 'upperdashtime',
  tempVar: ':upperDashboardTime:',
  type: TemplateType.Constant,
  values: [
    {
      value: 'now()',
      type: TemplateValueType.Constant,
      selected: true,
      localSelected: true,
    },
  ],
  label: '',
}
export const predefinedTemplateVariables: Template[] = [
  {...dashtimeTempVar},
  {...upperdashtimeTempVar},
  {...interval},
]

export const thresholdsListColors: ColorNumber[] = [
  {
    type: 'text',
    hex: '#00C9FF',
    id: 'base',
    name: 'laser',
    value: -1000000000000000000,
  },
]

export const gaugeColors: ColorNumber[] = [
  {
    type: 'min',
    hex: '#00C9FF',
    id: '0',
    name: 'laser',
    value: 0,
  },
  {
    type: 'max',
    hex: '#9394FF',
    id: '1',
    name: 'comet',
    value: 100,
  },
]
