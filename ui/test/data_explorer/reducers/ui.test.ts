import reducer from 'src/data_explorer/reducers/ui'

import {addQuery, deleteQuery} from 'src/data_explorer/actions/view'

let state

describe('DataExplorer.Reducers.UI', () => {
  it('it can add a query', () => {
    const actual = reducer(state, addQuery())
    expect(actual.queryIDs.length).toBe(1)
  })

  it('it can delete a query', () => {
    const queryID = '123'
    state = {queryIDs: ['456', queryID]}

    const actual = reducer(state, deleteQuery(queryID))
    const expected = {
      queryIDs: ['456'],
    }

    expect(actual).toEqual(expected)
  })
})
