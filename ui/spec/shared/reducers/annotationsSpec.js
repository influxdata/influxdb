import reducer from 'shared/reducers/annotations'

import {loadAnnotations} from 'shared/actions/annotations'

describe('Shared.Reducers.annotations', () => {
  it('can load the annotations', () => {
    let state = []
    const expected = [{time: '0', duration: ''}]
    const actual = reducer(state, loadAnnotations(expected))

    expect(actual).to.deep.equal(expected)
  })
})
