export const source = {
  id: '2',
  name: 'minikube-influx',
  type: 'influx',
  url: 'http://192.168.99.100:30400',
  'default': true,
  telegraf: 'telegraf',
  organization: 'default',
  role: 'viewer',
  links: {
    self: '/chronograf/v1/sources/2',
    kapacitors: '/chronograf/v1/sources/2/kapacitors',
    proxy: '/chronograf/v1/sources/2/proxy',
    queries: '/chronograf/v1/sources/2/queries',
    write: '/chronograf/v1/sources/2/write',
    permissions: '/chronograf/v1/sources/2/permissions',
    users: '/chronograf/v1/sources/2/users',
    databases: '/chronograf/v1/sources/2/dbs',
    annotations: '/chronograf/v1/sources/2/annotations'
  }
}

export const kapacitor = {
  id: '1',
  name: 'Test Kapacitor',
  url: 'http://localhost:9092',
  insecureSkipVerify: false,
  active: true,
  links: {
    self: '/chronograf/v1/sources/47/kapacitors/1',
    proxy: '/chronograf/v1/sources/47/kapacitors/1/proxy',
  },
}

export const createKapacitorBody = {
  name: 'Test Kapacitor',
  url: 'http://localhost:9092',
  insecureSkipVerify: false,
  username: 'user',
  password: 'pass',
}
