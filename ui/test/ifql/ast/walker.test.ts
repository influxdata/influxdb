import Walker from 'src/ifql/ast/walker'
import From from 'test/ifql/ast/from'
import Complex from 'test/ifql/ast/complex'
import Variable from 'test/ifql/ast/variable'

describe('IFQL.AST.Walker', () => {
  describe('Walker#functions', () => {
    describe('simple example', () => {
      describe('a single expression', () => {
        it('returns a flattened ordered list of from and its arguments', () => {
          const walker = new Walker(From)
          expect(walker.stuff).toEqual([
            {
              source: 'from(db: "telegraf")',
              funcs: [
                {
                  name: 'from',
                  source: 'from(db: "telegraf")',
                  arguments: [
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

        describe('a single variable declaration', () => {
          it('returns a variable declaration for a string literal', () => {
            const walker = new Walker(Variable)
            expect(walker.stuff).toEqual([
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
      })
    })

    describe('complex example', () => {
      it('returns a flattened ordered list of all funcs and their arguments', () => {
        const walker = new Walker(Complex)
        expect(walker.stuff).toEqual([
          {
            source:
              'from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "cpu") |> range(start: -1m)',
            funcs: [
              {
                name: 'from',
                source: 'from(db: "telegraf")',
                arguments: [{key: 'db', value: 'telegraf'}],
              },
              {
                name: 'filter',
                source: '|> filter(fn: (r) => r["_measurement"] == "cpu")',
                arguments: [
                  {
                    key: 'fn',
                    value: '(r) => r["_measurement"] == "cpu"',
                  },
                ],
              },
              {
                name: 'range',
                source: '|> range(start: -1m)',
                arguments: [{key: 'start', value: '-1m'}],
              },
            ],
          },
        ])
      })
    })
  })
})
