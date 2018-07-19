import Walker from 'src/flux/ast/walker'
import From from 'test/flux/ast/from'
import Complex from 'test/flux/ast/complex'
import {
  StringLiteral,
  Expression,
  ArrowFunction,
  Fork,
  JoinWithObjectArg,
} from 'test/flux/ast/variable'

describe('Flux.AST.Walker', () => {
  describe('Walker#functions', () => {
    describe('simple example', () => {
      describe('a single expression', () => {
        it('returns a flattened ordered list of from and its args', () => {
          const walker = new Walker(From)
          const expectedWalkerBody = [
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
          ]
          expect(walker.body).toEqual(expectedWalkerBody)
        })

        describe('variables', () => {
          describe('a single string literal variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(StringLiteral)
              const expectedWalkerBody = [
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
              ]
              expect(walker.body).toEqual(expectedWalkerBody)
            })
          })

          describe('a single expression variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(Expression)
              const expectedWalkerBody = [
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
              ]
              expect(walker.body).toEqual(expectedWalkerBody)
            })
          })

          describe('a single ArrowFunction variable', () => {
            it('returns the expected list', () => {
              const walker = new Walker(ArrowFunction)
              const expectedWalkerBody = [
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
              ]
              expect(walker.body).toEqual(expectedWalkerBody)
            })
          })

          describe('forking', () => {
            it('return the expected list of objects', () => {
              const walker = new Walker(Fork)
              const expectedWalkerBody = [
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
              ]
              expect(walker.body).toEqual(expectedWalkerBody)
            })
          })
        })
      })
    })

    describe('Args that are objects', () => {
      it('returns an object when arg type is object', () => {
        const walker = new Walker(JoinWithObjectArg)
        const expectedWalkerBody = [
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
        ]
        expect(walker.body).toEqual(expectedWalkerBody)
      })
    })

    describe('complex example', () => {
      it('returns a flattened ordered list of all funcs and their args', () => {
        const walker = new Walker(Complex)
        const expectedWalkerBody = [
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
        ]
        expect(walker.body).toEqual(expectedWalkerBody)
      })
    })
  })
})
