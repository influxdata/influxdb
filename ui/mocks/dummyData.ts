import {Template, SourceLinks, TemplateType, TemplateValueType} from 'src/types'
import {Source} from '@influxdata/influx'
import {Cell, Dashboard, Label} from 'src/types/v2'
import {Links} from 'src/types/v2/links'
import {Task} from 'src/types/v2/tasks'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {WithRouterProps} from 'react-router'
import {ConfigurationState} from 'src/types/v2/dataLoaders'
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputRedis,
  TelegrafPluginInputDisk,
  TelegrafPluginInputDiskio,
  TelegrafPluginInputMem,
  TelegrafPluginInputSystem,
  TelegrafPluginInputProcesses,
  TelegrafPluginInputNet,
  TelegrafPluginInputProcstat,
  TelegrafPluginInputDocker,
  TelegrafPluginInputSwap,
  Task as TaskApi,
  Organization,
  Variable,
} from '@influxdata/influx'

export const links: Links = {
  authorizations: '/api/v2/authorizations',
  buckets: '/api/v2/buckets',
  dashboards: '/api/v2/dashboards',
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  variables: '/api/v2/variables',
  me: '/api/v2/me',
  orgs: '/api/v2/orgs',
  query: {
    ast: '/api/v2/query/ast',
    self: '/api/v2/query',
    spec: '/api/v2/query/spec',
    suggestions: '/api/v2/query/suggestions',
  },
  setup: '/api/v2/setup',
  signin: '/api/v2/signin',
  signout: '/api/v2/signout',
  sources: '/api/v2/sources',
  system: {
    debug: '/debug/pprof',
    health: '/health',
    metrics: '/metrics',
  },
  tasks: '/api/v2/tasks',
  users: '/api/v2/users',
  views: '/api/v2/views',
  write: '/api/v2/write',
  defaultDashboard: '/v2/dashboards/029d13fda9c5b000',
}

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
  type: Source.TypeEnum.Self,
  username: 'admin',
  url: 'https://localhost:9086',
  insecureSkipVerify: true,
  telegraf: 'telegraf',
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
  id: '1',
  orgID: '02ee9e2a29d73000',
  cells: [],
  name: 'd1',
  links: {
    self: 'self/link',
    cells: 'cells/link',
  },
  meta: {
    createdAt: '2019-01-08T11:57:31.562044-08:00',
    updatedAt: '2019-01-08T12:57:31.562048-08:00',
  },
  labels: [],
}

export const labels: Label[] = [
  {
    id: '0001',
    name: 'Trogdor',
    properties: {
      color: '#44ffcc',
      description: 'Burninating the countryside',
    },
  },
  {
    id: '0002',
    name: 'Strawberry',
    properties: {
      color: '#ff0054',
      description: 'It is a great fruit',
    },
  },
]

export const dashboardWithLabels: Dashboard = {
  id: '1',
  cells: [],
  name: 'd1',
  orgID: '02ee9e2a29d73000',
  links: {
    self: 'self/link',
    cells: 'cells/link',
  },
  meta: {
    createdAt: '2019-01-08T11:57:31.562044-08:00',
    updatedAt: '2019-01-08T12:57:31.562048-08:00',
  },
  labels,
}

export const cell: Cell = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  id: '0246e457-916b-43e3-be99-211c4cbc03e8',
  dashboardID: 'dummyDashboardID',
  links: {
    self: 'self/link',
    view: 'view/link',
  },
}

export const orgs: Organization[] = [
  {
    links: {
      buckets: '/api/v2/buckets?org=RadicalOrganization',
      dashboards: '/api/v2/dashboards?org=RadicalOrganization',
      self: '/api/v2/orgs/02ee9e2a29d73000',
      tasks: '/api/v2/tasks?org=RadicalOrganization',
    },
    id: '02ee9e2a29d73000',
    name: 'RadicalOrganization',
  },
]

export const tasks: Task[] = [
  {
    id: '02ef9deff2141000',
    orgID: '02ee9e2a29d73000',
    name: 'pasdlak',
    status: TaskApi.StatusEnum.Active,
    flux:
      'option task = {\n  name: "pasdlak",\n  cron: "2 0 * * *"\n}\nfrom(bucket: "inbucket") \n|> range(start: -1h)',
    cron: '2 0 * * *',
    organization: orgs[0],
    labels: [],
  },
  {
    id: '02f12c50dba72000',
    orgID: '02ee9e2a29d73000',
    name: 'somename',
    status: TaskApi.StatusEnum.Active,
    flux:
      'option task = {\n  name: "somename",\n  every: 1m,\n}\nfrom(bucket: "inbucket") \n|> range(start: -task.every)',
    every: '1m0s',
    organization: {
      links: {
        buckets: '/api/v2/buckets?org=RadicalOrganization',
        dashboards: '/api/v2/dashboards?org=RadicalOrganization',
        self: '/api/v2/orgs/02ee9e2a29d73000',
        tasks: '/api/v2/tasks?org=RadicalOrganization',
      },
      id: '02ee9e2a29d73000',
      name: 'RadicalOrganization',
    },
    labels,
  },
]

export const variables: Variable[] = [
  {
    name: 'a little variable',
    orgID: '0',
    arguments: {
      type: 'query',
      values: {query: '1 + 1 ', language: 'flux'},
    },
  },
]

export const defaultOnboardingStepProps: OnboardingStepProps = {
  links,
  currentStepIndex: 0,
  onSetCurrentStepIndex: jest.fn(),
  onIncrementCurrentStepIndex: jest.fn(),
  onDecrementCurrentStepIndex: jest.fn(),
  onSetStepStatus: jest.fn(),
  stepStatuses: [],
  stepTitles: [],
  setupParams: {username: '', password: '', org: '', bucket: ''},
  handleSetSetupParams: jest.fn(),
  notify: jest.fn(),
  onCompleteSetup: jest.fn(),
  onExit: jest.fn(),
  onSetSubstepIndex: jest.fn(),
}

export const withRouterProps: WithRouterProps = {
  params: {},
  location: null,
  routes: null,
  router: null,
}

export const token =
  'm4aUjEIhM758JzJgRmI6f3KNOBw4ZO77gdwERucF0bj4QOLHViD981UWzjaxW9AbyA5THOMBp2SVZqzbui2Ehw=='

export const telegrafConfigID = '030358c935b18000'

export const cpuPlugin = {
  name: 'cpu',
  type: 'input',
  comment: 'this is a test',
  config: {},
}

export const telegrafPlugin = {
  name: TelegrafPluginInputCpu.NameEnum.Cpu,
  configured: ConfigurationState.Unconfigured,
  active: false,
}

export const cpuTelegrafPlugin = {
  ...telegrafPlugin,
  configured: ConfigurationState.Configured,
}

export const diskTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputDisk.NameEnum.Disk,
  configured: ConfigurationState.Configured,
}

export const diskioTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputDiskio.NameEnum.Diskio,
  configured: ConfigurationState.Configured,
}

export const netTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputNet.NameEnum.Net,
  configured: ConfigurationState.Configured,
}

export const memTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputMem.NameEnum.Mem,
  configured: ConfigurationState.Configured,
}

export const processesTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputProcesses.NameEnum.Processes,
  configured: ConfigurationState.Configured,
}

export const procstatTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputProcstat.NameEnum.Procstat,
  configured: ConfigurationState.Unconfigured,
}

export const systemTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputSystem.NameEnum.System,
  configured: ConfigurationState.Configured,
}

export const redisTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputRedis.NameEnum.Redis,
}

export const swapTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputSwap.NameEnum.Swap,
  configured: ConfigurationState.Configured,
}

export const redisPlugin = {
  name: TelegrafPluginInputRedis.NameEnum.Redis,
  type: TelegrafPluginInputRedis.TypeEnum.Input,
  config: {
    servers: [],
    password: '',
  },
}

export const dockerTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputDocker.NameEnum.Docker,
  configured: ConfigurationState.Configured,
}

export const influxDB2Plugin = {
  name: 'influxdb_v2',
  type: 'output',
  comment: 'write to influxdb v2',
  config: {
    urls: ['http://127.0.0.1:9999'],
    token,
    organization: 'default',
    bucket: 'defbuck',
  },
}

export const telegrafConfig = {
  id: telegrafConfigID,
  organizationID: '1',
  name: 'in n out',
  created: '2018-11-28T18:56:48.854337-08:00',
  lastModified: '2018-11-28T18:56:48.854337-08:00',
  lastModifiedBy: '030358b695318000',
  agent: {collectionInterval: 15},
  plugins: [cpuPlugin, influxDB2Plugin],
}

export const getTelegrafConfigsResponse = {
  data: {
    configurations: [telegrafConfig],
  },
  status: 200,
  statusText: 'OK',
  headers: {
    date: 'Thu, 29 Nov 2018 18:10:21 GMT',
    'content-length': '570',
    'content-type': 'application/json; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    headers: {Accept: 'application/json, text/plain, */*'},
    method: 'get',
    url: '/api/v2/telegrafs?org=',
  },
  request: {},
}

export const createTelegrafConfigResponse = {
  data: telegrafConfig,
}

export const authResponse = {
  data: {
    links: {self: '/api/v2/authorizations'},
    authorizations: [
      {
        links: {
          self: '/api/v2/authorizations/030358b6aa718000',
          user: '/api/v2/users/030358b695318000',
        },
        id: '030358b6aa718000',
        token,
        status: 'active',
        user: 'iris',
        userID: '030358b695318000',
        permissions: [
          {action: 'create', resource: 'user'},
          {action: 'delete', resource: 'user'},
          {action: 'write', resource: 'org'},
          {action: 'write', resource: 'bucket/030358b6aa318000'},
        ],
      },
    ],
  },
  status: 200,
  statusText: 'OK',
  headers: {
    date: 'Thu, 29 Nov 2018 18:10:21 GMT',
    'content-length': '522',
    'content-type': 'application/json; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    headers: {Accept: 'application/json, text/plain, */*'},
    method: 'get',
    url: '/api/v2/authorizations?user=',
  },
  request: {},
}

export const bucket = {
  links: {
    labels: '/api/v2/buckets/034a10d6f7a6b000/labels',
    log: '/api/v2/buckets/034a10d6f7a6b000/log',
    org: '/api/v2/orgs/034a0adc49a6b000',
    self: '/api/v2/buckets/034a10d6f7a6b000',
  },
  id: '034a10d6f7a6b000',
  organizationID: '034a0adc49a6b000',
  organization: 'default',
  name: 'newbuck',
  retentionRules: [],
  labels: [],
}

export const buckets = [
  {
    links: {
      labels: '/api/v2/buckets/034a10d6f7a6b000/labels',
      log: '/api/v2/buckets/034a10d6f7a6b000/log',
      org: '/api/v2/orgs/034a0adc49a6b000',
      self: '/api/v2/buckets/034a10d6f7a6b000',
    },
    id: '034a10d6f7a6b000',
    organizationID: '034a0adc49a6b000',
    organization: 'default',
    name: 'newbuck',
    retentionRules: [],
    labels: [],
  },
  {
    links: {
      labels: '/api/v2/buckets/034a10d6f7a6b000/labels',
      log: '/api/v2/buckets/034a10d6f7a6b000/log',
      org: '/api/v2/orgs/034a0adc49a6b000',
      self: '/api/v2/buckets/034a10d6f7a6b000',
    },
    id: '034a10d6f7a6b001',
    organizationID: '034a0adc49a6b000',
    organization: 'default',
    name: 'newbuck1',
    retentionRules: [],
    labels: [],
  },
]

export const setSetupParamsResponse = {
  data: {
    user: {
      links: {
        log: '/api/v2/users/033bc62520fe3000/log',
        self: '/api/v2/users/033bc62520fe3000',
      },
      id: '033bc62520fe3000',
      name: 'iris',
    },
    bucket: {
      links: {
        labels: '/api/v2/buckets/033bc62534fe3000/labels',
        log: '/api/v2/buckets/033bc62534fe3000/log',
        org: '/api/v2/orgs/033bc62534be3000',
        self: '/api/v2/buckets/033bc62534fe3000',
      },
      id: '033bc62534fe3000',
      orgID: '033bc62534be3000',
      organization: 'default',
      name: 'defbuck',
      retentionRules: [],
      labels: [],
    },
    org: {
      links: {
        buckets: '/api/v2/buckets?org=default',
        dashboards: '/api/v2/dashboards?org=default',
        labels: '/api/v2/orgs/033bc62534be3000/labels',
        log: '/api/v2/orgs/033bc62534be3000/log',
        members: '/api/v2/orgs/033bc62534be3000/members',
        secrets: '/api/v2/orgs/033bc62534be3000/secrets',
        self: '/api/v2/orgs/033bc62534be3000',
        tasks: '/api/v2/tasks?org=default',
      },
      id: '033bc62534be3000',
      name: 'default',
    },
    auth: {
      id: '033bc62534fe3001',
      token:
        'GSEx9BfvjlwQZfjoMgYX9rARwK2Nzc2jaiLdZso9E6X9K1ymldtQ3DwYbCqV3ClJ47sXdI1nLzsP2C1S4u76hA==',
      status: 'active',
      description: "iris's Token",
      orgID: '033bc62534be3000',
      org: 'default',
      userID: '033bc62520fe3000',
      user: 'iris',
      permissions: [
        {
          action: 'read',
          resource: 'authorizations',
          orgID: '033bc62534be3000',
        },
        {
          action: 'write',
          resource: 'authorizations',
          orgID: '033bc62534be3000',
        },
        {action: 'read', resource: 'buckets', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'buckets', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'dashboards', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'dashboards', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'orgs', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'orgs', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'sources', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'sources', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'tasks', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'tasks', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'telegrafs', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'telegrafs', orgID: '033bc62534be3000'},
        {action: 'read', resource: 'users', orgID: '033bc62534be3000'},
        {action: 'write', resource: 'users', orgID: '033bc62534be3000'},
      ],
      links: {
        self: '/api/v2/authorizations/033bc62534fe3001',
        user: '/api/v2/users/033bc62520fe3000',
      },
    },
  },
  status: 201,
  statusText: 'Created',
  headers: {
    'access-control-allow-origin': 'http://localhost:9999',
    date: 'Fri, 11 Jan 2019 22:49:33 GMT',
    'access-control-allow-headers':
      'Accept, Content-Type, Content-Length, Accept-Encoding, Authorization',
    'transfer-encoding': 'chunked',
    'access-control-allow-methods': 'POST, GET, OPTIONS, PUT, DELETE',
    'content-type': 'application/json; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    headers: {
      Accept: 'application/json, text/plain, */*',
      'Content-Type': 'application/json',
    },
    method: 'post',
    data:
      '{"username":"iris","password":"iris","org":"default","bucket":"defbuck"}',
    url: '/api/v2/setup',
  },
  request: {},
}

export const telegraf = [
  {
    id: '03636a150fb51000',
    name: 'Name this Configuration',
    organizationID: '03636a0aabb51000',
  },
  {
    id: '03636a150fb51001',
    name: 'Name this Configuration',
    organizationID: '03636a0aabb51000',
  },
]

export const scraperTargets = [
  {
    bucket: 'a',
    bucketID: '03636a0aabb51001',
    id: '03636a0bfe351000',
    name: 'new target',
    orgID: '03636a0aabb51000',
    organization: 'a',
    type: 'prometheus',
    url: 'http://localhost:9999/metrics',
  },
  {
    bucket: 'a',
    bucketID: '03636a0aabb51001',
    id: '03636a0bfe351001',
    name: 'new target',
    orgID: '03636a0aabb51000',
    organization: 'a',
    type: 'prometheus',
    url: 'http://localhost:9999/metrics',
  },
]
