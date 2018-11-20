import {
  buildTableQueryConfig,
  buildLogQuery,
  validateTailQuery,
  validateOlderQuery,
} from 'src/logs/utils/logQuery'

import {oneline} from 'src/logs/utils/helpers/formatting'

import {QueryConfig} from 'src/types'
import {Filter, LogQuery} from 'src/types/logs'
import {InfluxLanguage} from 'src/types/v2/dashboards'

describe('Logs.LogQuery', () => {
  let config: QueryConfig
  let filters: Filter[]
  let lower: string
  let upper: string
  let logQuery: LogQuery

  beforeEach(() => {
    config = buildTableQueryConfig({
      id: '1',
      organization: 'default',
      organizationID: '1',
      name: 'telegraf',
      rp: 'autogen',
      retentionRules: [],
      links: {
        self: '',
        org: '',
      },
    })

    filters = []
    lower = '2018-10-10T22:46:24.859Z'
    upper = '2018-10-10T22:46:54.859Z'

    const source = {
      id: '1',
      name: 'foo',
      type: 'test',
      url: 'test.local',
      insecureSkipVerify: false,
      default: true,
      telegraf: 'telegraf.local',
      links: {
        self: 'test',
        query: 'test/query',
        buckets: 'test/buckets',
        health: 'test/health',
      },
    }

    logQuery = {lower, upper, filters, config, source}
  })

  it('can build a flux query', () => {
    const actual = buildLogQuery(InfluxLanguage.Flux, {
      lower,
      upper,
      config,
      filters,
    })

    const expected = oneline`
      from(bucket: "telegraf/autogen")
        |> range(start: 2018-10-10T22:46:24.859Z, stop: 2018-10-10T22:46:54.859Z)
        |> filter(fn: (r) => r._measurement == "syslog")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> group(none: true)
        |> sort(columns: ["_time"])
        |> map(fn: (r) => ({time: r._time,
            severity: r.severity,
            timestamp: r.timestamp,
            message: r.message,
            facility: r.facility,
            procid: r.procid,
            appname: r.appname,
            host: r.host}))
    `

    expect(actual).toEqual(expected)
  })

  it('can build an influxql query', () => {
    filters = [{key: 'severity', operator: '==', value: 'notice', id: '1'}]
    const actual = buildLogQuery(InfluxLanguage.InfluxQL, {
      lower,
      upper,
      config,
      filters,
    })

    const expected = oneline`
    SELECT
      "_time" AS "time",
      "severity" AS "severity",
      "timestamp" AS "timestamp",
      "message" AS "message",
      "facility" AS "facility",
      "procid" AS "procid",
      "appname" AS "appname",
      "host" AS "host"
    FROM
      "telegraf"."autogen"."syslog"
    WHERE
      time >= '2018-10-10T22:46:24.859Z' AND
      time < '2018-10-10T22:46:54.859Z' AND
      "severity" = 'notice'
    ORDER BY time DESC
  `

    expect(actual).toEqual(expected)
  })

  describe('validateTailQuery', () => {
    it('can error when id is incorrect', () => {
      const currentID = 3
      const id = 2
      const {error} = validateTailQuery(logQuery, id, currentID)

      expect(error).toEqual('Stale log tail')
    })

    it('can error when no currentID', () => {
      const currentID = undefined
      const id = 2
      const {error} = validateTailQuery(logQuery, id, currentID)

      expect(error).toEqual('Stale log tail')
    })
  })

  describe('validateOlderQuery', () => {
    it('can error when id is incorrect', () => {
      const currentID = 'currentID'
      const id = 'staleID'
      const {error} = validateOlderQuery(logQuery, id, currentID)

      expect(error).toEqual('Stale older batch request')
    })

    it('can error when no currentID', () => {
      const currentID = undefined
      const id = 'staleID'
      const {error} = validateOlderQuery(logQuery, id, currentID)

      expect(error).toEqual('Stale older batch request')
    })
  })
})
