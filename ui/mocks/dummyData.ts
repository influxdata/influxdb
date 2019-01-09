import {
  Source,
  SourceAuthenticationMethod,
  Template,
  Dashboard,
  Cell,
  SourceLinks,
  TemplateType,
  TemplateValueType,
} from 'src/types'
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
  Task as TaskApi,
  Label,
} from 'src/api'

export const links: Links = {
  authorizations: '/api/v2/authorizations',
  buckets: '/api/v2/buckets',
  dashboards: '/api/v2/dashboards',
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  macros: '/api/v2/macros',
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
  id: '1',
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
    resourceID: 'dashboard-mock-label-a',
    name: 'Trogdor',
    properties: {
      color: '#44ffcc',
      description: 'Burninating the countryside',
    },
  },
  {
    resourceID: 'dashboard-mock-label-b',
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
  viewID: 'view-1',
  id: '0246e457-916b-43e3-be99-211c4cbc03e8',
  links: {
    self: 'self/link',
    view: 'view/link',
  },
}

export const tasks: Task[] = [
  {
    id: '02ef9deff2141000',
    organizationID: '02ee9e2a29d73000',
    name: 'pasdlak',
    status: TaskApi.StatusEnum.Active,
    owner: {id: '02ee9e2a19d73000', name: ''},
    flux:
      'option task = {\n  name: "pasdlak",\n  cron: "2 0 * * *"\n}\nfrom(bucket: "inbucket") \n|> range(start: -1h)',
    cron: '2 0 * * *',
    organization: {
      links: {
        buckets: '/api/v2/buckets?org=RadicalOrganization',
        dashboards: '/api/v2/dashboards?org=RadicalOrganization',
        log: '/api/v2/orgs/02ee9e2a29d73000/log',
        members: '/api/v2/orgs/02ee9e2a29d73000/members',
        self: '/api/v2/orgs/02ee9e2a29d73000',
        tasks: '/api/v2/tasks?org=RadicalOrganization',
      },
      id: '02ee9e2a29d73000',
      name: 'RadicalOrganization',
    },
    labels: [],
  },
  {
    id: '02f12c50dba72000',
    organizationID: '02ee9e2a29d73000',
    name: 'somename',
    status: TaskApi.StatusEnum.Active,
    owner: {id: '02ee9e2a19d73000', name: ''},
    flux:
      'option task = {\n  name: "somename",\n  every: 1m,\n}\nfrom(bucket: "inbucket") \n|> range(start: -task.every)',
    every: '1m0s',
    organization: {
      links: {
        buckets: '/api/v2/buckets?org=RadicalOrganization',
        dashboards: '/api/v2/dashboards?org=RadicalOrganization',
        log: '/api/v2/orgs/02ee9e2a29d73000/log',
        members: '/api/v2/orgs/02ee9e2a29d73000/members',
        self: '/api/v2/orgs/02ee9e2a29d73000',
        tasks: '/api/v2/tasks?org=RadicalOrganization',
      },
      id: '02ee9e2a29d73000',
      name: 'RadicalOrganization',
    },
    labels,
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
