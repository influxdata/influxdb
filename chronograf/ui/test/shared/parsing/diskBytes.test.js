import {
  diskBytesFromShard,
  diskBytesFromShardForDatabase,
} from 'shared/parsing/diskBytes'

describe('diskBytesFromShard', () => {
  it('sums all the disk bytes in multiple series', () => {
    const response = {
      results: [
        {
          series: [
            {
              name: 'shard',
              tags: {
                clusterID: '6272208615254493595',
                database: '_internal',
                engine: 'tsm1',
                hostname: 'WattsInfluxDB',
                id: '1',
                nodeID: 'localhost:8088',
                path: '/Users/watts/.influxdb/data/_internal/monitor/1',
                retentionPolicy: 'monitor',
              },
              columns: ['time', 'last'],
              values: [[1464811503000000000, 100]],
            },
          ],
        },
        {
          series: [
            {
              name: 'shard',
              tags: {
                clusterID: '6272208615254493595',
                database: 'telegraf',
                engine: 'tsm1',
                hostname: 'WattsInfluxDB',
                id: '2',
                nodeID: 'localhost:8088',
                path: '/Users/watts/.influxdb/data/telegraf/default/2',
                retentionPolicy: 'default',
              },
              columns: ['time', 'last'],
              values: [[1464811503000000000, 200]],
            },
          ],
        },
      ],
    }

    const result = diskBytesFromShard(response)
    const expectedTotal = 300

    expect(result.errors).toEqual([])
    expect(result.bytes).toBe(expectedTotal)
  })

  it('returns emtpy with empty response', () => {
    const response = {results: [{}]}

    const result = diskBytesFromShard(response)

    expect(result.errors).toEqual([])
    expect(result.bytes).toBe(0)
  })

  it('exposes the server error', () => {
    const response = {results: [{error: 'internal server error?'}]}

    const result = diskBytesFromShard(response)

    expect(result.errors).toEqual(['internal server error?'])
    expect(result.bytes).toBe(0)
  })
})

describe('diskBytesFromShardForDatabase', () => {
  it('return parses data as expected', () => {
    const response = {
      results: [
        {
          series: [
            {
              name: 'shard',
              tags: {
                nodeID: 'localhost:8088',
                path: '/Users/watts/.influxdb/data/_internal/monitor/1',
                retentionPolicy: 'monitor',
              },
              columns: ['time', 'last'],
              values: [['2016-06-02T01:06:13Z', 100]],
            },
            {
              name: 'shard',
              tags: {
                nodeID: 'localhost:8088',
                path: '/Users/watts/.influxdb/data/_internal/monitor/3',
                retentionPolicy: 'monitor',
              },
              columns: ['time', 'last'],
              values: [['2016-06-02T01:06:13Z', 200]],
            },
            {
              name: 'shard',
              tags: {
                nodeID: 'localhost:8188',
                path: '/Users/watts/.influxdb/data/_internal/monitor/1',
                retentionPolicy: 'monitor',
              },
              columns: ['time', 'last'],
              values: [['2016-06-02T01:06:13Z', 100]],
            },
            {
              name: 'shard',
              tags: {
                nodeID: 'localhost:8188',
                path: '/Users/watts/.influxdb/data/_internal/monitor/3',
                retentionPolicy: 'monitor',
              },
              columns: ['time', 'last'],
              values: [['2016-06-02T01:06:13Z', 200]],
            },
          ],
        },
      ],
    }

    const result = diskBytesFromShardForDatabase(response)
    const expected = {
      1: [
        {nodeID: 'localhost:8088', diskUsage: 100},
        {nodeID: 'localhost:8188', diskUsage: 100},
      ],
      3: [
        {nodeID: 'localhost:8088', diskUsage: 200},
        {nodeID: 'localhost:8188', diskUsage: 200},
      ],
    }

    expect(result.shardData).toEqual(expected)
  })

  it('returns emtpy with empty response', () => {
    const response = {results: [{}]}

    const result = diskBytesFromShardForDatabase(response)

    expect(result.errors).toEqual([])
    expect(result.shardData).toEqual({})
  })

  it('exposes the server error', () => {
    const response = {results: [{error: 'internal server error?'}]}

    const result = diskBytesFromShardForDatabase(response)

    expect(result.errors).toEqual(['internal server error?'])
    expect(result.shardData).toEqual({})
  })
})
