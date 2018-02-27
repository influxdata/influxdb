import reducer from 'shared/reducers/sources'

import {loadSources, updateSource, addSource} from 'shared/actions/sources'

describe('Shared.Reducers.sources', () => {
  it('can correctly show default sources when adding a source', () => {
    let state = []

    state = reducer(
      state,
      addSource({
        id: '1',
        default: true,
      })
    )

    state = reducer(
      state,
      addSource({
        id: '2',
        default: true,
      })
    )

    expect(state.filter(s => s.default).length).to.equal(1)
  })

  it('can correctly show default sources when updating a source', () => {
    let state = []

    state = reducer(
      state,
      addSource({
        id: '1',
        default: true,
      })
    )

    state = reducer(
      state,
      addSource({
        id: '2',
        default: true,
      })
    )

    state = reducer(
      state,
      updateSource({
        id: '1',
        default: true,
      })
    )

    expect(state.find(({id}) => id === '1').default).to.equal(true)
    expect(state.find(({id}) => id === '2').default).to.equal(false)
  })
})
