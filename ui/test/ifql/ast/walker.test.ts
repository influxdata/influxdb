import Walker from 'src/ifql/ast/walker'
import From from 'test/ifql/ast/from'
import Complex from 'test/ifql/ast/complex'
import {
  StringLiteral,
  Expression,
  ArrowFunction,
  Fork,
  JoinWithObjectArg,
} from 'test/ifql/ast/variable'

describe('IFQL.AST.Walker', () => {
  describe('Walker#functions', () => {
    describe('simple example', () => {
      describe('a single expression', () => {
        it('returns a flattened ordered list of from and its args', () => {
          const walker = new Walker(From)
          expect(walker.body).toEqual([
            {
              type: 'CallExpression',
              source: 'from(db: "telegraf")',
              funcs: [
                {
                  name: 'from',
                  source: 'from(db: "telegraf")',
                  args: [
                    {
                      key: 'db',
                      value: 'telegraf',
                    },
                  ],
                },
              ],
            },
          ])
        })

        describe('variables', () => {
          describe('a single string literal variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(StringLiteral)
              expect(walker.body).toEqual([
                {
                  type: 'VariableDeclaration',
                  source: 'bux = "im a var"',
                  declarations: [
                    {
                      name: 'bux',
                      type: 'StringLiteral',
                      value: 'im a var',
                    },
                  ],
                },
              ])
            })
          })

          describe('a single expression variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(Expression)
              expect(walker.body).toEqual([
                {
                  type: 'VariableDeclaration',
                  source: 'tele = from(db: "telegraf")',
                  declarations: [
                    {
                      name: 'tele',
                      type: 'CallExpression',
                      source: 'tele = from(db: "telegraf")',
                      funcs: [
                        {
                          name: 'from',
                          source: 'from(db: "telegraf")',
                          args: [
                            {
                              key: 'db',
                              value: 'telegraf',
                            },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ])
            })
          })

          describe('a single ArrowFunction variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(ArrowFunction)
              expect(walker.body).toEqual([
                {
                  type: 'VariableDeclaration',
                  source: 'addOne = (n) => n + 1',
                  declarations: [
                    {
                      name: 'addOne',
                      type: 'ArrowFunctionExpression',
                      source: '(n) => n + 1',
                      params: [{type: 'Property', source: 'n'}],
                      body: [
                        {type: 'Identifier', source: 'n'},
                        {type: 'Operator', source: '+'},
                        {type: 'IntegerLiteral', source: '1'},
                      ],
                    },
                  ],
                },
              ])
            })
          })

          describe('forking', () => {
            it('return the expected list of objects', () => {
              const walker = new Walker(Fork)
              expect(walker.body).toEqual([
                {
                  type: 'VariableDeclaration',
                  source: 'tele = from(db: "telegraf")',
                  declarations: [
                    {
                      name: 'tele',
                      type: 'CallExpression',
                      source: 'tele = from(db: "telegraf")',
                      funcs: [
                        {
                          name: 'from',
                          source: 'from(db: "telegraf")',
                          args: [
                            {
                              key: 'db',
                              value: 'telegraf',
                            },
                          ],
                        },
                      ],
                    },
                  ],
                },
                {
                  type: 'PipeExpression',
                  source: 'tele |> sum()',
                  funcs: [
                    {args: [], name: 'tele', source: 'tele'},
                    {args: [], name: 'sum', source: '|> sum()'},
                  ],
                },
              ])
            })
          })
        })
      })
    })

    describe('Args that are objects', () => {
      it('returns an object when arg type is object', () => {
        const walker = new Walker(JoinWithObjectArg)
        expect(walker.body).toEqual([
          {
            type: 'CallExpression',
            source:
              'join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])',
            funcs: [
              {
                name: 'join',
                source:
                  'join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])',
                args: [
                  {key: 'tables', value: {cpu: 'cpu', mem: 'mem'}},
                  {key: 'on', value: ['host']},
                  {
                    key: 'fn',
                    value:
                      '(tables) => tables.cpu["_value"] + tables.mem["_value"]',
                  },
                ],
              },
            ],
          },
        ])
      })
    })

    describe('complex example', () => {
      it('returns a flattened ordered list of all funcs and their args', () => {
        const walker = new Walker(Complex)
        expect(walker.body).toEqual([
          {
            type: 'PipeExpression',
            source:
              'from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "cpu") |> range(start: -1m)',
            funcs: [
              {
                name: 'from',
                source: 'from(db: "telegraf")',
                args: [{key: 'db', value: 'telegraf'}],
              },
              {
                name: 'filter',
                source: '|> filter(fn: (r) => r["_measurement"] == "cpu")',
                args: [
                  {
                    key: 'fn',
                    value: '(r) => r["_measurement"] == "cpu"',
                  },
                ],
              },
              {
                name: 'range',
                source: '|> range(start: -1m)',
                args: [{key: 'start', value: '-1m'}],
              },
            ],
          },
        ])
      })
    })
  })
})
