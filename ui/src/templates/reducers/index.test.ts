import templatesReducer, {defaultState} from 'src/templates/reducers'
import {setTemplateSummary} from 'src/templates/actions'

describe('templatesReducer', () => {
  describe('setTemplateSummary', () => {
    it('can update the name of a template', () => {
      const initialState = defaultState()
      const initialTemplate = {
        id: 'abc',
        labels: [],
        meta: {name: 'Belcalis', version: '1'},
      }
      initialState.items.push(initialTemplate)

      const actual = templatesReducer(
        initialState,
        setTemplateSummary(initialTemplate.id, {
          ...initialTemplate,
          meta: {...initialTemplate.meta, name: 'Cardi B'},
        })
      )

      const expected = {
        ...defaultState(),
        items: [
          {
            ...initialTemplate,
            meta: {...initialTemplate.meta, name: 'Cardi B'},
          },
        ],
      }

      expect(actual).toEqual(expected)
    })
  })
})
