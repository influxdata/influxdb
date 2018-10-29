import InfluxQL from 'src/influxql'

describe('influxql astToString', () => {
  it('simple query', () => {
    const ast = InfluxQL({
      fields: [
        {
          column: {
            expr: 'binary',
            op: '+',
            lhs: {
              expr: 'literal',
              val: '1',
              type: 'integer',
            },
            rhs: {
              expr: 'reference',
              val: 'A',
            },
          },
        },
      ],
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'howdy',
          type: 'measurement',
        },
      ],
    })

    const expected = `SELECT 1 + "A" FROM "howdy"`
    const actual = ast.toString()

    // console.log(actual)

    expect(actual).toBe(expected)
  })

  it('simple query w/ multiple sources', () => {
    const ast = InfluxQL({
      fields: [
        {
          column: {
            expr: 'binary',
            op: '+',
            lhs: {
              expr: 'literal',
              val: '1',
              type: 'integer',
            },
            rhs: {
              expr: 'reference',
              val: 'A',
            },
          },
        },
      ],
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'howdy',
          type: 'measurement',
        },
        {
          database: 'telegraf',
          retentionPolicy: 'autogen',
          name: 'doody',
          type: 'measurement',
        },
      ],
    })

    const expected = `SELECT 1 + "A" FROM "howdy", "telegraf"."autogen"."doody"`
    const actual = ast.toString()

    // console.log('actual  ', actual)
    // console.log('expected', expected)

    expect(actual).toBe(expected)
  })

  it('query with AS', () => {
    const ast = InfluxQL({
      fields: [
        {
          alias: 'B',
          column: {
            expr: 'binary',
            op: '+',
            lhs: {
              expr: 'literal',
              val: '1',
              type: 'integer',
            },
            rhs: {
              expr: 'reference',
              val: 'A',
            },
          },
        },
      ],
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'howdy',
          type: 'measurement',
        },
      ],
    })

    const expected = `SELECT 1 + "A" AS "B" FROM "howdy"`
    const actual = ast.toString()

    // console.log(actual)

    expect(actual).toBe(expected)
  })

  it('query with 2x func', () => {
    const ast = InfluxQL({
      fields: [
        {
          column: {
            expr: 'binary',
            op: '/',
            lhs: {
              expr: 'call',
              name: 'derivative',
              args: [
                {
                  expr: 'reference',
                  val: 'field1',
                },
                {
                  expr: 'literal',
                  val: '1h',
                  type: 'duration',
                },
              ],
            },
            rhs: {
              expr: 'call',
              name: 'derivative',
              args: [
                {
                  expr: 'reference',
                  val: 'field2',
                },
                {
                  expr: 'literal',
                  val: '1h',
                  type: 'duration',
                },
              ],
            },
          },
        },
      ],
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'myseries',
          type: 'measurement',
        },
      ],
    })

    const expected = `SELECT derivative("field1", 1h) / derivative("field2", 1h) FROM "myseries"`
    const actual = ast.toString()

    expect(actual).toBe(expected)
  })

  it('query with where and groupby', () => {
    const ast = InfluxQL({
      condition: {
        expr: 'binary',
        op: 'AND',
        lhs: {
          expr: 'binary',
          op: 'AND',
          lhs: {
            expr: 'binary',
            op: '=~',
            lhs: {
              expr: 'reference',
              val: 'cluster_id',
            },
            rhs: {
              expr: 'literal',
              val: '/^23/',
              type: 'regex',
            },
          },
          rhs: {
            expr: 'binary',
            op: '=',
            lhs: {
              expr: 'reference',
              val: 'host',
            },
            rhs: {
              expr: 'literal',
              val: 'prod-2ccccc04-us-east-1-data-3',
              type: 'string',
            },
          },
        },
        rhs: {
          expr: 'binary',
          op: '\u003e',
          lhs: {
            expr: 'reference',
            val: 'time',
          },
          rhs: {
            expr: 'binary',
            op: '-',
            lhs: {
              expr: 'call',
              name: 'now',
            },
            rhs: {
              expr: 'literal',
              val: '15m',
              type: 'duration',
            },
          },
        },
      },
      fields: [
        {
          alias: 'max_cpus',
          column: {
            expr: 'call',
            name: 'max',
            args: [
              {
                expr: 'reference',
                val: 'n_cpus',
              },
            ],
          },
        },
        {
          column: {
            expr: 'call',
            name: 'non_negative_derivative',
            args: [
              {
                expr: 'call',
                name: 'median',
                args: [
                  {
                    expr: 'reference',
                    val: 'n_users',
                  },
                ],
              },
              {
                expr: 'literal',
                val: '5m',
                type: 'duration',
              },
            ],
          },
        },
      ],
      groupBy: {
        time: {
          interval: '15m',
          offset: '10s',
        },
        tags: ['host', 'tag_x'],
        fill: '10',
      },
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'system',
          type: 'measurement',
        },
      ],
    })

    const expected =
      'SELECT max("n_cpus") AS "max_cpus", non_negative_derivative(median("n_users"), 5m) FROM "system" WHERE "cluster_id" =~ /^23/ AND "host" = \'prod-2ccccc04-us-east-1-data-3\' AND time > now() - 15m GROUP BY time(15m, 10s),host,tag_x fill(10)'
    const actual = ast.toString()

    // console.log('actual  ', actual)
    // console.log('expected', expected)

    expect(actual).toBe(expected)
  })

  it('query with orderby and limit', () => {
    const ast = InfluxQL({
      condition: {
        expr: 'binary',
        op: 'AND',
        lhs: {
          expr: 'binary',
          op: '=',
          lhs: {
            expr: 'reference',
            val: 'host',
          },
          rhs: {
            expr: 'literal',
            val: 'hosta.influxdb.org',
            type: 'string',
          },
        },
        rhs: {
          expr: 'binary',
          op: '\u003e',
          lhs: {
            expr: 'reference',
            val: 'time',
          },
          rhs: {
            expr: 'literal',
            val: '2017-02-07T01:43:02.245407693Z',
            type: 'string',
          },
        },
      },
      fields: [
        {
          column: {
            expr: 'call',
            name: 'mean',
            args: [
              {
                expr: 'reference',
                val: 'field1',
              },
            ],
          },
        },
        {
          column: {
            expr: 'call',
            name: 'sum',
            args: [
              {
                expr: 'reference',
                val: 'field2',
              },
            ],
          },
        },
        {
          alias: 'field_x',
          column: {
            expr: 'call',
            name: 'count',
            args: [
              {
                expr: 'reference',
                val: 'field3',
              },
            ],
          },
        },
      ],
      groupBy: {
        time: {
          interval: '10h',
        },
      },
      limits: {
        limit: 20,
        offset: 10,
      },
      orderbys: [
        {
          name: 'time',
          order: 'descending',
        },
      ],
      sources: [
        {
          database: '',
          retentionPolicy: '',
          name: 'myseries',
          type: 'measurement',
        },
      ],
    })

    const expected = `SELECT mean("field1"), sum("field2"), count("field3") AS "field_x" FROM "myseries" WHERE "host" = 'hosta.influxdb.org' AND time > '2017-02-07T01:43:02.245407693Z' GROUP BY time(10h) ORDER BY time DESC LIMIT 20 OFFSET 10`
    const actual = ast.toString()

    // console.log('actual  ', actual)
    // console.log('expected', expected)

    expect(actual).toBe(expected)
  })
})
