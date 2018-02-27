import reducer from 'src/data_explorer/reducers/timeRange'

import {setTimeRange} from 'src/data_explorer/actions/view'

const noopAction = () => {
  return {type: 'NOOP'}
}

describe('DataExplorer.Reducers.TimeRange', () => {
  it('it sets the default timeRange', () => {
    const state = reducer(undefined, noopAction())
    const expected = {
      lower: 'now() - 1h',
      upper: null,
    }

    expect(state).to.deep.equal(expected)
  })

  it('it can set the time range', () => {
    const timeRange = {
      lower: 'now() - 5m',
      upper: null,
    }
    const expected = reducer(undefined, setTimeRange(timeRange))

    expect(timeRange).to.deep.equal(expected)
  })
})
