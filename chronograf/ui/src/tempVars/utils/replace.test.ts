import templateReplace, {replaceInterval} from 'src/tempVars/utils/replace'
import {TemplateType, TemplateValueType} from 'src/types/tempVars'

describe('templates.utils.replace', () => {
  it('can replace select with parameters', () => {
    const vars = [
      {
        id: '1',
        type: TemplateType.CSV,
        label: '',
        query: {},
        tempVar: ':temperature:',
        values: [
          {
            type: TemplateValueType.CSV,
            value: '10',
            selected: false,
            localSelected: true,
          },
        ],
      },
      {
        id: '2',
        type: TemplateType.FieldKeys,
        label: '',
        query: {},
        tempVar: ':field:',
        values: [
          {
            type: TemplateValueType.FieldKey,
            value: 'field2',
            selected: true,
            localSelected: false,
          },
        ],
      },
      {
        id: '3',
        type: TemplateType.CSV,
        label: '',
        query: {},
        tempVar: ':method:',
        values: [
          {
            type: TemplateValueType.CSV,
            value: 'SELECT',
            selected: true,
            localSelected: false,
          },
        ],
      },
      {
        id: '4',
        type: TemplateType.CSV,
        label: '',
        query: {},
        tempVar: ':measurement:',
        values: [
          {
            type: TemplateValueType.CSV,
            value: `"cpu"`,
            selected: false,
            localSelected: true,
          },
        ],
      },
    ]
    const query =
      ':method: field1, :field: FROM :measurement: WHERE temperature > :temperature:'
    const expected = `SELECT field1, "field2" FROM "cpu" WHERE temperature > 10`

    const actual = templateReplace(query, vars)
    expect(actual).toBe(expected)
  })

  it('can replace all in a select with parameters and aggregates', () => {
    const vars = [
      {
        id: '1',
        type: TemplateType.TagValues,
        label: '',
        query: {},
        tempVar: ':value:',
        values: [
          {
            type: TemplateValueType.TagValue,
            value: 'howdy.com',
            selected: false,
            localSelected: true,
          },
          {
            type: TemplateValueType.TagValue,
            value: 'nope',
            selected: false,
            localSelected: false,
          },
        ],
      },
      {
        id: '2',
        type: TemplateType.TagKeys,
        label: '',
        query: {},
        tempVar: ':tag:',
        values: [
          {
            type: TemplateValueType.TagKey,
            value: 'host',
            selected: false,
            localSelected: true,
          },
          {
            type: TemplateValueType.TagKey,
            value: 'nope',
            selected: false,
            localSelected: false,
          },
        ],
      },
      {
        id: '3',
        type: TemplateType.FieldKeys,
        label: '',
        query: {},
        tempVar: ':field:',
        values: [
          {
            type: TemplateValueType.FieldKey,
            value: 'field',
            selected: true,
            localSelected: false,
          },
          {
            type: TemplateValueType.FieldKey,
            value: 'nope',
            selected: false,
            localSelected: false,
          },
        ],
      },
    ]

    const query = `SELECT mean(:field:) FROM "cpu" WHERE :tag: = :value: GROUP BY :tag:`
    const expected = `SELECT mean("field") FROM "cpu" WHERE "host" = 'howdy.com' GROUP BY "host"`
    const actual = templateReplace(query, vars)

    expect(actual).toBe(expected)
  })

  describe('queries with a regex', () => {
    it('replaces properly', () => {
      const vars = [
        {
          id: '1',
          type: TemplateType.TagValues,
          label: '',
          query: {},
          tempVar: ':host:',
          values: [
            {
              type: TemplateValueType.TagValue,
              value: 'my-host.local',
              selected: true,
              localSelected: false,
            },
            {
              type: TemplateValueType.TagValue,
              value: 'my-host.urban',
              selected: false,
              localSelected: false,
            },
          ],
        },
        {
          id: '2',
          type: TemplateType.TagValues,
          label: '',
          query: {},
          tempVar: ':region:',
          values: [
            {
              type: TemplateValueType.TagValue,
              value: 'north',
              selected: false,
              localSelected: true,
            },
            {
              type: TemplateValueType.TagValue,
              value: 'south',
              selected: false,
              localSelected: false,
            },
          ],
        },
        {
          id: '3',
          type: TemplateType.Constant,
          label: '',
          query: {},
          tempVar: ':dashboardTime:',
          values: [
            {
              value: 'now() - 1h',
              type: TemplateValueType.Constant,
              selected: false,
              localSelected: true,
            },
            {
              value: 'now() - 2h',
              type: TemplateValueType.Constant,
              selected: false,
              localSelected: false,
            },
          ],
        },
      ]

      const query = `SELECT "usage_active" FROM "cpu" WHERE host =~ /^:host:$/ AND host = :host: AND region =~ /:region:/ AND time > :dashboardTime: FILL(null)`
      const expected = `SELECT "usage_active" FROM "cpu" WHERE host =~ /^my-host.local$/ AND host = 'my-host.local' AND region =~ /north/ AND time > now() - 1h FILL(null)`
      const actual = templateReplace(query, vars)

      expect(actual).toBe(expected)
    })
  })

  describe('with no templates', () => {
    it('does not do a replacement', () => {
      const query = `SELECT :field: FROM "cpu"`
      const expected = query
      const actual = templateReplace(query, [])

      expect(actual).toBe(expected)
    })
  })

  describe('with no template values', () => {
    it('does not do a replacement', () => {
      const vars = [
        {
          id: '1',
          type: TemplateType.CSV,
          label: '',
          query: {},
          tempVar: ':field:',
          values: [],
        },
      ]
      const query = `SELECT :field: FROM "cpu"`
      const expected = query
      const actual = templateReplace(query, vars)

      expect(actual).toBe(expected)
    })
  })

  describe('replaceInterval', () => {
    it('can replace :interval:', () => {
      const query = `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(:interval:)`
      const expected = `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(46702702ms)`
      const pixels = 333
      const durationMs = 15551999999
      const actual = replaceInterval(query, pixels, durationMs)

      expect(actual).toBe(expected)
    })

    it('can replace multiple intervals', () => {
      const query = `SELECT NON_NEGATIVE_DERIVATIVE(mean(usage_idle), :interval:) from "cpu" where time > now() - 4320h group by time(:interval:)`
      const expected = `SELECT NON_NEGATIVE_DERIVATIVE(mean(usage_idle), 46702702ms) from "cpu" where time > now() - 4320h group by time(46702702ms)`

      const pixels = 333
      const durationMs = 15551999999
      const actual = replaceInterval(query, pixels, durationMs)

      expect(actual).toBe(expected)
    })

    describe('when used with other template variables', () => {
      it('can work with :dashboardTime:', () => {
        const vars = [
          {
            id: '1',
            type: TemplateType.Constant,
            label: '',
            query: {},
            tempVar: ':dashboardTime:',
            values: [
              {
                type: TemplateValueType.Constant,
                value: 'now() - 24h',
                selected: true,
                localSelected: false,
              },
              {
                type: TemplateValueType.Constant,
                value: 'now() - 5h',
                selected: false,
                localSelected: false,
              },
            ],
          },
        ]

        const pixels = 333
        const durationMs = 86399999
        const query = `SELECT mean(usage_idle) from "cpu" WHERE time > :dashboardTime: group by time(:interval:)`
        let actual = templateReplace(query, vars)
        actual = replaceInterval(actual, pixels, durationMs)
        const expected = `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 24h group by time(259459ms)`

        expect(actual).toBe(expected)
      })

      it('can handle a failing condition', () => {
        const vars = [
          {
            id: '1',
            type: TemplateType.Constant,
            label: '',
            query: {},
            tempVar: ':dashboardTime:',
            values: [
              {
                type: TemplateValueType.Constant,
                value: 'now() - 1h',
                selected: true,
                localSelected: false,
              },
              {
                type: TemplateValueType.Constant,
                value: 'now() - 2h',
                selected: false,
                localSelected: false,
              },
            ],
          },
        ]

        const pixels = 38
        const durationMs = 3599999
        const query = `SELECT mean(usage_idle) from "cpu" WHERE time > :dashboardTime: group by time(:interval:)`
        let actual = templateReplace(query, vars)
        actual = replaceInterval(actual, pixels, durationMs)
        const expected = `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 1h group by time(94736ms)`

        expect(actual).toBe(expected)
      })
    })

    describe('with no :interval: present', () => {
      it('returns the query', () => {
        const expected = `SELECT mean(usage_idle) FROM "cpu" WHERE time > :dashboardTime: GROUP BY time(20ms)`
        const actual = replaceInterval(expected, 10, 20000)

        expect(actual).toBe(expected)
      })
    })
  })

  describe('with order-dependent template variables', () => {
    it('can render a query with a nested CSV query', () => {
      const templates = [
        {
          id: '95281aaa-4330-4f07-a22a-b16bebbcfc6b',
          tempVar: ':filterByHost:',
          type: TemplateType.CSV,
          label: '',
          query: {},
          values: [
            {
              value: "AND host = ':host:'",
              type: TemplateValueType.CSV,
              selected: true,
              localSelected: true,
            },
          ],
        },
        {
          id: 'b865afa5-f54c-49a8-adfa-9435450bac3a',
          tempVar: ':host:',
          type: TemplateType.MetaQuery,
          label: '',
          query: {
            influxql:
              'SHOW TAG VALUES ON "telegraf" FROM "cpu" WITH KEY = "host"',
          },
          values: [
            {
              type: TemplateValueType.MetaQuery,
              value: 'myhost.local',
              selected: true,
              localSelected: true,
            },
          ],
        },
      ]
      const query = `SELECT mean("usage_user") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 30m :filterByHost: GROUP BY time(1s)`
      const expected = `SELECT mean("usage_user") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 30m AND host = 'myhost.local' GROUP BY time(1s)`

      expect(templateReplace(query, templates)).toEqual(expected)

      // Should not be dependent on order
      expect(templateReplace(query, templates.reverse())).toEqual(expected)
    })

    it('can render a query with a nested map query', () => {
      const templates = [
        {
          id: '95281aaa-4330-4f07-a22a-b16bebbcfc6b',
          tempVar: ':filterByHost:',
          type: TemplateType.Map,
          label: '',
          query: {},
          values: [
            {
              value: "AND host = ':host:'",
              key: 'with host',
              type: TemplateValueType.Map,
              selected: false,
              localSelected: true,
            },
            {
              value: 'AND 1 = 1',
              key: 'without host',
              type: TemplateValueType.Map,
              selected: true,
              localSelected: false,
            },
          ],
        },
        {
          id: 'b865afa5-f54c-49a8-adfa-9435450bac3a',
          tempVar: ':host:',
          type: TemplateType.MetaQuery,
          label: '',
          query: {
            influxql:
              'SHOW TAG VALUES ON "telegraf" FROM "cpu" WITH KEY = "host"',
          },
          values: [
            {
              type: TemplateValueType.MetaQuery,
              value: 'myhost.local',
              selected: true,
              localSelected: true,
            },
          ],
        },
      ]
      const query = `SELECT mean("usage_user") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 30m :filterByHost: GROUP BY time(1s)`
      const expected = `SELECT mean("usage_user") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 30m AND host = 'myhost.local' GROUP BY time(1s)`

      expect(templateReplace(query, templates)).toEqual(expected)

      // Should not be dependent on order
      expect(templateReplace(query, templates.reverse())).toEqual(expected)
    })
  })
})
