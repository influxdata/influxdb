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
  },
}
