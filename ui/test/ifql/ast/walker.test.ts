import Walker from 'src/ifql/ast/walker'
import From from 'test/ifql/ast/from'
import Complex from 'test/ifql/ast/complex'

describe('IFQL.AST.Walker', () => {
  describe('Walker#functions', () => {
    describe('simple example', () => {
      it('returns a flattened ordered list of from and its arguments', () => {
        const walker = new Walker(From)
        expect(walker.functions).toEqual([
          {
            name: 'from',
            arguments: [
              {
                name: 'db',
                value: 'telegraf',
              },
            ],
          },
        ])
      })
    })

    describe('complex example', () => {
      it('returns a flattened ordered list of all funcs and their arguments', () => {
        const walker = new Walker(Complex)
        expect(walker.functions).toEqual([
          {
            name: 'from',
            arguments: [{name: 'db', value: 'telegraf'}],
          },
          {
            name: 'filter',
            arguments: [
              {
                name: 'fn',
                value: '(r) => r["_measurement"] == "cpu"',
              },
            ],
          },
          {
            name: 'range',
            arguments: [{name: 'start', value: '-1m'}],
          },
        ])
      })
    })
  })
})
