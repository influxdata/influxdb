const showDatabasesResponse = {
  results: [
    {
      statement_id: 0,
      series: [
        {
          name: 'databases',
          columns: ['name'],
          values: [['telegraf'], ['_internal'], ['chronograf']],
        },
      ],
    },
  ],
}

export const showDatabases = jest.fn(() =>
  Promise.resolve({data: showDatabasesResponse})
)

const showRetentionPoliciesResponse = {
  results: [
    {
      statement_id: 0,
      series: [
        {
          columns: [
            'name',
            'duration',
            'shardGroupDuration',
            'replicaN',
            'default',
          ],
          values: [['autogen', '0s', '168h0m0s', 1, true]],
        },
      ],
    },
    {
      statement_id: 1,
      series: [
        {
          columns: [
            'name',
            'duration',
            'shardGroupDuration',
            'replicaN',
            'default',
          ],
          values: [['monitor', '168h0m0s', '24h0m0s', 1, true]],
        },
      ],
    },
    {
      statement_id: 2,
      series: [
        {
          columns: [
            'name',
            'duration',
            'shardGroupDuration',
            'replicaN',
            'default',
          ],
          values: [['autogen', '0s', '168h0m0s', 1, true]],
        },
      ],
    },
  ],
}

export const showRetentionPolicies = jest.fn(() =>
  Promise.resolve({data: showRetentionPoliciesResponse})
)

const showFieldKeysResponse = {
  data: {
    results: [
      {
        statement_id: 0,
        series: [
          {
            name: 'm1',
            columns: ['fieldKey', 'fieldType'],
            values: [
              ['usage_guest', 'float'],
              ['usage_guest_nice', 'float'],
              ['usage_idle', 'float'],
              ['usage_iowait', 'float'],
              ['usage_irq', 'float'],
              ['usage_nice', 'float'],
              ['usage_softirq', 'float'],
              ['usage_steal', 'float'],
              ['usage_system', 'float'],
              ['usage_user', 'float'],
            ],
          },
        ],
      },
    ],
  },
  status: 200,
  statusText: 'OK',
  headers: {
    date: 'Thu, 10 May 2018 00:31:31 GMT',
    'content-encoding': 'gzip',
    vary: 'Accept-Encoding',
    'content-length': '171',
    'x-chronograf-version': '1.4.3.0-1103-gb964e64fd',
    'content-type': 'application/json',
  },
  config: {self: '/chronograf/v1/config', auth: '/chronograf/v1/config/auth'},
  request: {},
  auth: {links: []},
  external: {statusFeed: 'https://www.influxdata.com/feed/json'},
  users: '/chronograf/v1/organizations/default/users',
  allUsers: '/chronograf/v1/users',
  organizations: '/chronograf/v1/organizations',
  meLink: '/chronograf/v1/me',
  environment: '/chronograf/v1/env',
  ifql: {
    ast: '/chronograf/v1/ifql/ast',
    self: '/chronograf/v1/ifql',
    suggestions: '/chronograf/v1/ifql/suggestions',
  },
}

export const showFieldKeys = jest.fn(() =>
  Promise.resolve(showFieldKeysResponse)
)
