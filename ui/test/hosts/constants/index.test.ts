import {generateTempVarsForHosts} from 'src/hosts/constants'

describe('hosts.constants.index', () => {
  describe('generateTempVarsForHosts', () => {
    it('should return template variables', () => {
      const telegraf = 'bob'
      const defaultRP = '1h10m'
      const source = {telegraf, defaultRP}

      const expected = [
        {
          tempVar: ':db:',
          id: 'db',
          type: 'constant',
          values: [{value: telegraf, type: 'constant', selected: true}],
        },
        {
          tempVar: ':rp:',
          id: 'rp',
          type: 'constant',
          values: [{value: defaultRP, type: 'constant', selected: true}],
        },
      ]
      const actual = generateTempVarsForHosts(source)

      expect(actual).toEqual(expected)
    })

    describe('if rp is an empty string', () => {
      it('should return an empty rention policy variable', () => {
        const telegraf = 'bob'
        const defaultRP = ''
        const source = {telegraf, defaultRP}

        const expected = [
          {
            tempVar: ':db:',
            id: 'db',
            type: 'constant',
            values: [{value: telegraf, type: 'constant', selected: true}],
          },
          {
            tempVar: ':rp:',
            id: 'rp',
            type: 'constant',
            values: [{value: '', type: 'constant', selected: true}],
          },
        ]
        const actual = generateTempVarsForHosts(source)

        expect(actual).toEqual(expected)
      })
    })
  })
})
