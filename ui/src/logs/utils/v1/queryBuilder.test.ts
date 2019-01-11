import {buildInfluxQLQuery} from 'src/logs/utils/v1/queryBuilder'
import {buildTableQueryConfig} from 'src/logs/utils/logQuery'
import {oneline} from 'src/logs/utils/helpers/formatting'

import {QueryConfig} from 'src/types'
import {Filter} from 'src/types/logs'

describe('Logs.V1.queryBuilder', () => {
  let config: QueryConfig
  let filters: Filter[]
  let lower: string
  let upper: string

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
  })

  it('can build a query config into a query', () => {
    const actual = buildInfluxQLQuery({
      lower,
      upper,
      filters,
      config,
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
        time < '2018-10-10T22:46:54.859Z'
    `

    expect(actual).toEqual(expected)
  })

  it('can build a query config into a query with a filter', () => {
    filters = [{key: 'severity', operator: '!=', value: 'notice', id: '1'}]
    const actual = buildInfluxQLQuery({
      lower,
      upper,
      filters,
      config,
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
            "severity" != 'notice'
    `

    expect(actual).toEqual(expected)
  })

  it('can build a query config into a query with multiple filters', () => {
    filters = [
      {key: 'severity', operator: '==', value: 'notice', id: '1'},
      {key: 'appname', operator: '!~', value: 'beep', id: '1'},
      {key: 'appname', operator: '=~', value: 'o_trace_id=broken', id: '1'},
    ]

    const actual = buildInfluxQLQuery({
      lower,
      upper,
      filters,
      config,
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
            "severity" = 'notice' AND
            "appname" !~ /beep/ AND
            "appname" =~ /o_trace_id=broken/
    `

    expect(actual).toEqual(expected)
  })
})
