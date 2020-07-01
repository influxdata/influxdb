import {ViewProperties} from 'src/client'
import {
  Cell,
  Dashboard,
  Task,
  Links,
  ConfigurationState,
  RemoteDataState,
  Label,
} from 'src/types'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {RouteComponentProps} from 'react-router-dom'
import {NumericColumnData} from '@influxdata/giraffe'
import {
  Source,
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
  Authorization,
  AuthorizationUpdateRequest,
  Permission,
  PermissionResource,
} from '@influxdata/influx'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'
import {DashboardSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

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

export const source: Source = {
  id: '16',
  name: 'ssl',
  type: Source.TypeEnum.Self,
  username: 'admin',
  url: 'https://localhost:9086',
  insecureSkipVerify: true,
  telegraf: 'telegraf',
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

const defaultSortOptions = {
  sortDirection: Sort.Ascending,
  sortType: SortTypes.String,
  sortKey: 'name' as DashboardSortKey,
}

// Dashboards
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
  status: RemoteDataState.Done,
  sortOptions: defaultSortOptions,
}

export const labels: Label[] = [
  {
    id: '0001',
    name: 'Trogdor',
    properties: {
      color: '#44ffcc',
      description: 'Burninating the countryside',
    },
    status: RemoteDataState.Done,
  },
  {
    id: '0002',
    name: 'Strawberry',
    properties: {
      color: '#ff0054',
      description: 'It is a great fruit',
    },
    status: RemoteDataState.Done,
  },
]

const labelIDs = labels.map(l => l.id)

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
  status: RemoteDataState.Done,
  labels: labelIDs,
  sortOptions: defaultSortOptions,
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
  status: RemoteDataState.Done,
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
    org: 'default',
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
    org: 'default',
    labels: labelIDs,
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
  stepTestIds: [],
  setupParams: {username: '', password: '', org: '', bucket: ''},
  handleSetSetupParams: jest.fn(),
  notify: jest.fn(),
  onCompleteSetup: jest.fn(),
  onExit: jest.fn(),
  onSetSubstepIndex: jest.fn(),
}

export const withRouterProps: RouteComponentProps = {
  match: {},
  location: null,
  routes: null,
  history: null,
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
  templateID: '0000000000000009',
}

export const diskTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputDisk.NameEnum.Disk,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
}

export const diskioTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputDiskio.NameEnum.Diskio,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
}

export const netTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputNet.NameEnum.Net,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
}

export const memTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputMem.NameEnum.Mem,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
}

export const processesTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputProcesses.NameEnum.Processes,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
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
  templateID: '0000000000000009',
}

export const redisTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputRedis.NameEnum.Redis,
  templateID: '0000000000000008',
}

export const swapTelegrafPlugin = {
  ...telegrafPlugin,
  name: TelegrafPluginInputSwap.NameEnum.Swap,
  configured: ConfigurationState.Configured,
  templateID: '0000000000000009',
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
  templateID: '0000000000000002',
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
  orgID: '1',
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
  orgID: '034a0adc49a6b000',
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
    orgID: '034a0adc49a6b000',
    name: 'newbuck',
    retentionRules: [],
    readableRetention: 'forever',
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
    orgID: '034a0adc49a6b000',
    name: 'newbuck1',
    retentionRules: [],
    readableRetention: 'forever',
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
    orgID: '03636a0aabb51000',
  },
  {
    id: '03636a150fb51001',
    name: 'Name this Configuration',
    orgID: '03636a0aabb51000',
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

export const auth: Authorization = {
  id: '03c03a8a64728000',
  token:
    'RcW2uWiD-vfxujKyJCirK8un3lJsWPfiA6ulmWY_SlSITUal7Z180OwExiKKfrO98X8W6qGrd5hSGdag-hEpWw==',
  status: AuthorizationUpdateRequest.StatusEnum.Active,
  description: 'My token',
  orgID: '039edab314789000',
  org: 'a',
  userID: '039edab303789000',
  user: 'adminuser',
  permissions: [
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Orgs,
        id: '039edab314789000',
        name: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Authorizations,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Authorizations,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Buckets,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Buckets,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Dashboards,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Dashboards,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Sources,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Sources,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Tasks,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Tasks,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Telegrafs,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Telegrafs,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Users,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Users,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Variables,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Variables,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Scrapers,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Scrapers,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Secrets,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Secrets,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Labels,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Labels,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Views,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Views,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Read,
      resource: {
        type: PermissionResource.TypeEnum.Documents,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
    {
      action: Permission.ActionEnum.Write,
      resource: {
        type: PermissionResource.TypeEnum.Documents,
        orgID: '039edab314789000',
        org: 'a',
      },
    },
  ],
  links: {
    self: '/api/v2/authorizations/03c03a8a64728000',
    user: '/api/v2/users/039edab303789000',
  },
}

export const viewProperties: ViewProperties = {
  shape: 'chronograf-v2',
  queries: [
    {
      text:
        'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "used_percent")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\n  |> yield(name: "mean")',
      editMode: 'advanced',
      name: '',
      builderConfig: {
        buckets: [],
        tags: [
          {
            key: '_measurement',
            values: [],
            aggregateFunctionType: 'filter',
          },
        ],
        functions: [],
        aggregateWindow: {
          period: '',
        },
      },
    },
  ],
  axes: {
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
      suffix: '%',
      base: '10',
      scale: 'linear',
    },
  },
  type: 'line-plus-single-stat',
  legend: {},
  colors: [
    {
      id: 'base',
      type: 'text',
      hex: '#00C9FF',
      name: 'laser',
      value: 0,
    },
    {
      id: '1ce2dd3d-ece9-4305-b938-5b1538063119',
      type: 'scale',
      hex: '#8F8AF4',
      name: 'Do Androids Dream of Electric Sheep?',
      value: 0,
    },
    {
      id: '2e1d1dbf-6ed3-4978-9622-2a90548363a9',
      type: 'scale',
      hex: '#A51414',
      name: 'Do Androids Dream of Electric Sheep?',
      value: 0,
    },
    {
      id: 'edda21a2-1c61-40df-9c2f-c85e16978548',
      type: 'scale',
      hex: '#F4CF31',
      name: 'Do Androids Dream of Electric Sheep?',
      value: 0,
    },
  ],
  prefix: '',
  suffix: '%',
  decimalPlaces: {
    isEnforced: true,
    digits: 1,
  },
  note: '',
  showNoteWhenEmpty: false,
  xColumn: '_time',
  yColumn: '_value',
  shadeBelow: true,
  hoverDimension: 'y',
  position: 'overlaid',
}

export const numericColumnData: NumericColumnData = [
  1573766950000,
  1573766950000,
  1573766960000,
  1573766970000,
  1573766980000,
  1573766990000,
  1573767000000,
  1573767010000,
  1573767020000,
  1573767030000,
  1573767040000,
  1573767050000,
  1573767060000,
  1573767070000,
  1573767080000,
  1573767090000,
  1573767100000,
  1573767110000,
  1573767120000,
  1573767130000,
  1573767140000,
  1573767150000,
  1573767160000,
  1573767170000,
  1573767180000,
  1573767190000,
  1573767200000,
  1573767210000,
  1573767220000,
  1573767230000,
  1573767240000,
  1573767250000,
  1573767260000,
  1573767270000,
  1573767280000,
  1573767290000,
  1573767300000,
  1573767310000,
  1573767320000,
  1573767330000,
  1573767340000,
  1573767350000,
  1573767360000,
  1573767370000,
  1573767380000,
  1573767390000,
  1573767400000,
  1573767410000,
  1573767420000,
  1573767430000,
  1573767440000,
  1573767450000,
  1573767460000,
  1573767470000,
  1573767480000,
  1573767490000,
  1573767500000,
  1573767510000,
  1573767520000,
  1573767530000,
  1573767540000,
  1573767550000,
  1573767560000,
  1573767570000,
  1573767580000,
  1573767590000,
  1573767600000,
  1573767610000,
  1573767620000,
  1573767630000,
  1573767640000,
  1573767650000,
  1573767660000,
  1573767670000,
  1573767680000,
  1573767690000,
  1573767700000,
  1573767710000,
  1573767720000,
  1573767730000,
  1573767740000,
  1573767750000,
  1573767760000,
  1573767770000,
  1573767780000,
  1573767790000,
  1573767800000,
  1573767810000,
  1573767820000,
  1573767830000,
  1573767840000,
  1573767850000,
  1573767860000,
  1573767870000,
  1573767880000,
  1573767890000,
  1573767900000,
  1573767910000,
  1573767920000,
  1573767930000,
  1573767940000,
]
