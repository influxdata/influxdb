export const telegrafConfigID = '030358c935b18000'

export const telegrafConfig = {
  id: telegrafConfigID,
  name: 'in n out',
  created: '2018-11-28T18:56:48.854337-08:00',
  lastModified: '2018-11-28T18:56:48.854337-08:00',
  lastModifiedBy: '030358b695318000',
  agent: {collectionInterval: 15},
  plugins: [
    {name: 'cpu', type: 'input', comment: 'this is a test', config: {}},
    {
      name: 'influxdb_v2',
      type: 'output',
      comment: 'write to influxdb v2',
      config: {
        urls: ['http://127.0.0.1:9999'],
        token:
          'm4aUjEIhM758JzJgRmI6f3KNOBw4ZO77gdwERucF0bj4QOLHViD981UWzjaxW9AbyA5THOMBp2SVZqzbui2Ehw==',
        organization: 'default',
        bucket: 'defbuck',
      },
    },
  ],
}

export const telegrafConfigsResponse = {
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

export const token =
  'm4aUjEIhM758JzJgRmI6f3KNOBw4ZO77gdwERucF0bj4QOLHViD981UWzjaxW9AbyA5THOMBp2SVZqzbui2Ehw=='

export const authResponse = {
  data: {
    links: {self: '/api/v2/authorizations'},
    auths: [
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
