import reducer, {initialState} from 'src/sources/reducers/sources'

import {updateSource, addSource, loadSources} from 'src/shared/actions/sources'

import {source} from 'src/sources/resources'

describe('sources reducer', () => {
  it('can LOAD_SOURCES', () => {
    const expected = [{...source, id: '1'}]
    const actual = reducer(initialState, loadSources(expected))

    expect(actual).toEqual(expected)
  })

  describe('ADD_SOURCES', () => {
    it('can ADD_SOURCES', () => {
      let state = []

      state = reducer(
        state,
        addSource({
          ...source,
          id: '1',
          default: true,
        })
      )

      state = reducer(
        state,
        addSource({
          ...source,
          id: '2',
          default: true,
        })
      )

      expect(state.filter(s => s.default).length).toBe(1)
    })

    it('can correctly show default sources when updating a source', () => {
      let state = []

      state = reducer(
        initialState,
        addSource({
          ...source,
          id: '1',
          default: true,
        })
      )

      state = reducer(
        state,
        addSource({
          ...source,
          id: '2',
          default: true,
        })
      )

      state = reducer(
        state,
        updateSource({
          ...source,
          id: '1',
          default: true,
        })
      )

      expect(state.find(({id}) => id === '1').default).toBe(true)
      expect(state.find(({id}) => id === '2').default).toBe(false)
    })
  })
})
