import templateReplace, {replaceInterval} from 'src/tempVars/utils/replace'
import {TemplateValueType} from 'src/types/tempVars'
import {emptyTemplate} from 'test/resources'

describe('templates.utils.replace', () => {
  it('can replace select with parameters', () => {
    const vars = [
      {
        ...emptyTemplate,
        tempVar: ':temperature:',
        values: [{type: TemplateValueType.CSV, value: '10', selected: true}],
      },
      {
        ...emptyTemplate,
        tempVar: ':field:',
        values: [
          {type: TemplateValueType.FieldKey, value: 'field2', selected: true},
        ],
      },
      {
        ...emptyTemplate,
        tempVar: ':method:',
        values: [
          {type: TemplateValueType.CSV, value: 'SELECT', selected: true},
        ],
      },
      {
        ...emptyTemplate,
        tempVar: ':measurement:',
        values: [{type: TemplateValueType.CSV, value: `"cpu"`, selected: true}],
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
        ...emptyTemplate,
        tempVar: ':value:',
        values: [
          {
            type: TemplateValueType.TagValue,
            value: 'howdy.com',
            selected: true,
          },
        ],
      },
      {
        ...emptyTemplate,
        tempVar: ':tag:',
        values: [
          {type: TemplateValueType.TagKey, value: 'host', selected: true},
        ],
      },
      {
        ...emptyTemplate,
        tempVar: ':field:',
        values: [
          {type: TemplateValueType.FieldKey, value: 'field', selected: true},
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
          ...emptyTemplate,
          tempVar: ':host:',
          values: [
            {
              type: TemplateValueType.TagValue,
              value: 'my-host.local',
              selected: true,
            },
          ],
        },
        {
          ...emptyTemplate,
          tempVar: ':region:',
          values: [
            {
              type: TemplateValueType.TagValue,
              value: 'north',
              selected: true,
            },
          ],
        },
        {
          ...emptyTemplate,
          tempVar: ':dashboardTime:',
          values: [
            {
              value: 'now() - 1h',
              type: TemplateValueType.Constant,
              selected: true,
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
          ...emptyTemplate,
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
            ...emptyTemplate,
            tempVar: ':dashboardTime:',
            values: [
              {
                type: TemplateValueType.Constant,
                value: 'now() - 24h',
                selected: true,
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
            ...emptyTemplate,
            tempVar: ':dashboardTime:',
            values: [
              {
                type: TemplateValueType.Constant,
                value: 'now() - 1h',
                selected: true,
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
})
