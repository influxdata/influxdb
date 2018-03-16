export const links = {
  self: '/chronograf/v1/sources/16',
  kapacitors: '/chronograf/v1/sources/16/kapacitors',
  proxy: '/chronograf/v1/sources/16/proxy',
  queries: '/chronograf/v1/sources/16/queries',
  write: '/chronograf/v1/sources/16/write',
  permissions: '/chronograf/v1/sources/16/permissions',
  users: '/chronograf/v1/sources/16/users',
  databases: '/chronograf/v1/sources/16/dbs',
}

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
  links,
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
