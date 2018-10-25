import {
  buildTableQueryConfig,
  buildInfiniteScrollLogQuery,
} from 'src/logs/utils/queryBuilder'

import {QueryConfig} from 'src/types'
import {Filter} from 'src/types/logs'

describe('Logs.queryBuilder', () => {
  let queryConfig: QueryConfig
  let filters: Filter[]
  let lower: string
  let upper: string

  beforeEach(() => {
    queryConfig = buildTableQueryConfig({
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
  })

  it('can build a query config into a query', () => {
    const actual = buildInfiniteScrollLogQuery(
      lower,
      upper,
      queryConfig,
      filters
    )
    const expected = [
      `from(bucket: "telegraf/autogen")`,
      `range(start: 2018-10-10T22:46:24.859Z, stop: 2018-10-10T22:46:54.859Z)`,
      `filter(fn: (r) => r._measurement == "syslog")`,
      `pivot(rowKey:["_time"], colKey: ["_field"], valueCol: "_value")`,
      `group(none: true)`,
      `sort(cols: ["_time"])`,
      `map(fn: (r) => ({time: r._time, severity: r.severity, timestamp: r.timestamp, message: r.message, facility: r.facility, procid: r.procid, appname: r.appname, host: r.host}))`,
    ].join('\n  |> ')

    expect(actual).toEqual(expected)
  })

  it('can build a query config into a query with a filter', () => {
    filters = [{key: 'severity', operator: '!=', value: 'notice', id: '1'}]
    const actual = buildInfiniteScrollLogQuery(
      lower,
      upper,
      queryConfig,
      filters
    )

    const expected = [
      `from(bucket: "telegraf/autogen")`,
      `range(start: 2018-10-10T22:46:24.859Z, stop: 2018-10-10T22:46:54.859Z)`,
      `filter(fn: (r) => r._measurement == "syslog")`,
      `pivot(rowKey:["_time"], colKey: ["_field"], valueCol: "_value")`,
      `group(none: true)`,
      `filter(fn: (r) => r.severity != "notice")`,
      `sort(cols: ["_time"])`,
      `map(fn: (r) => ({time: r._time, severity: r.severity, timestamp: r.timestamp, message: r.message, facility: r.facility, procid: r.procid, appname: r.appname, host: r.host}))`,
    ].join('\n  |> ')

    expect(actual).toEqual(expected)
  })

  it('can build a query config into a query with multiple filters', () => {
    filters = [
      {key: 'severity', operator: '==', value: 'notice', id: '1'},
      {key: 'appname', operator: '!~', value: 'beep', id: '1'},
      {key: 'appname', operator: '=~', value: 'o_trace_id=broken', id: '1'},
    ]

    const actual = buildInfiniteScrollLogQuery(
      lower,
      upper,
      queryConfig,
      filters
    )

    const expected = [
      `from(bucket: "telegraf/autogen")`,
      `range(start: 2018-10-10T22:46:24.859Z, stop: 2018-10-10T22:46:54.859Z)`,
      `filter(fn: (r) => r._measurement == "syslog")`,
      `pivot(rowKey:["_time"], colKey: ["_field"], valueCol: "_value")`,
      `group(none: true)`,
      `filter(fn: (r) => r.severity == "notice" and r.appname !~ /beep/ and r.appname =~ /o_trace_id=broken/)`,
      `sort(cols: ["_time"])`,
      `map(fn: (r) => ({time: r._time, severity: r.severity, timestamp: r.timestamp, message: r.message, facility: r.facility, procid: r.procid, appname: r.appname, host: r.host}))`,
    ].join('\n  |> ')

    expect(actual).toEqual(expected)
  })
})
