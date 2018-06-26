import templateReplace from 'src/tempVars/utils/replace'
import {TemplateValueType} from 'src/types/tempVars'

describe('templates.utils.replace', () => {
  describe('template replacement', () => {
    it('can replace select with parameters', () => {
      const query =
        ':method: field1, :field: FROM :measurement: WHERE temperature > :temperature:'

      const vars = [
        {
          tempVar: ':temperature:',
          values: [{type: TemplateValueType.CSV, value: '10', selected: true}],
        },
        {
          tempVar: ':field:',
          values: [
            {type: TemplateValueType.FieldKey, value: 'field2', selected: true},
          ],
        },
        {
          tempVar: ':method:',
          values: [
            {type: TemplateValueType.CSV, value: 'SELECT', selected: true},
          ],
        },
        {
          tempVar: ':measurement:',
          values: [
            {type: TemplateValueType.CSV, value: `"cpu"`, selected: true},
          ],
        },
      ]

      const expected = `SELECT field1, "field2" FROM "cpu" WHERE temperature > 10`

      const actual = templateReplace(query, vars)
      expect(actual).toBe(expected)
    })
  })

  it('can replace all in a select with paramaters and aggregates', () => {
    const vars = [
      {
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
        tempVar: ':tag:',
        values: [
          {type: TemplateValueType.TagKey, value: 'host', selected: true},
        ],
      },
      {
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

  describe('sad path', () => {
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
            tempVar: ':field:',
            values: [],
          },
        ]
        const query = `SELECT :field: FROM "cpu"`
        const expected = query
        const actual = templateReplace(query, [])

        expect(actual).toBe(expected)
      })
    })

    describe('with an unknown template type', () => {
      it('does not do a replacement', () => {
        const vars = [
          {
            tempVar: ':field:',
            values: [
              {
                type: 'howdy',
                value: 'field',
                selected: true,
              },
            ],
          },
        ]

        const query = `SELECT :field: FROM "cpu"`
        const expected = query
        const actual = templateReplace(query, vars)

        expect(actual).toBe(expected)
      })
    })
  })
})
