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
