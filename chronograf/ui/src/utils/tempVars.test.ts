import {generateForHosts} from 'src/utils/tempVars'
import {TemplateValueType, TemplateType} from 'src/types'
import {source} from 'mocks/dummy'

describe('utils.tempVars', () => {
  describe('generateForHosts', () => {
    it('should return template variables', () => {
      const telegraf = 'bob'
      const defaultRP = '1h10m'
      const thisSource = {...source, telegraf, defaultRP}

      const expected = [
        {
          tempVar: ':db:',
          id: 'db',
          label: '',
          type: TemplateType.Constant,
          values: [
            {
              value: telegraf,
              type: TemplateValueType.Constant,
              selected: true,
              localSelected: true,
            },
          ],
        },
        {
          tempVar: ':rp:',
          id: 'rp',
          label: '',
          type: TemplateType.Constant,
          values: [
            {
              value: defaultRP,
              type: TemplateValueType.Constant,
              selected: true,
              localSelected: true,
            },
          ],
        },
      ]
      const actual = generateForHosts(thisSource)

      expect(actual).toEqual(expected)
    })

    describe('if rp is an empty string', () => {
      it('should return an empty rention policy variable', () => {
        const telegraf = 'bob'
        const defaultRP = ''
        const thisSource = {...source, telegraf, defaultRP}

        const expected = [
          {
            tempVar: ':db:',
            id: 'db',
            label: '',
            type: TemplateType.Constant,
            values: [
              {
                value: telegraf,
                type: TemplateValueType.Constant,
                selected: true,
                localSelected: true,
              },
            ],
          },
          {
            tempVar: ':rp:',
            id: 'rp',
            label: '',
            type: TemplateType.Constant,
            values: [
              {
                value: '',
                type: TemplateValueType.Constant,
                selected: true,
                localSelected: true,
              },
            ],
          },
        ]
        const actual = generateForHosts(thisSource)

        expect(actual).toEqual(expected)
      })
    })
  })
})
