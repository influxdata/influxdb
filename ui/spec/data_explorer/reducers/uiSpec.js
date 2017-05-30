import reducer from 'src/data_explorer/reducers/ui'

import {addQuery, deleteQuery} from 'src/data_explorer/actions/view'

const noopAction = () => {
  return {type: 'NOOP'}
}

let state

describe('DataExplorer.Reducers.UI', () => {
  it('it sets the default state for UI', () => {
    const actual = reducer(state, noopAction())
    const expected = {
      queryIDs: [],
    }

    expect(actual).to.deep.equal(expected)
  })

  it('it can add a query', () => {
    const actual = reducer(state, addQuery())
    expect(actual.queryIDs.length).to.equal(1)
  })

  it('it can delete a query', () => {
    const queryID = '123'
    state = {queryIDs: ['456', queryID]}

    const actual = reducer(state, deleteQuery(queryID))
    const expected = {
      queryIDs: ['456'],
    }

    expect(actual).to.deep.equal(expected)
  })
})
