import reducer, {defaultState} from 'src/logs/reducers'
import {setTimeWindow} from 'src/logs/actions'

describe('Logs.Reducers', () => {
  it('can set a time window', () => {
    const expected = '1h'
    const actual = reducer(defaultState, setTimeWindow(expected))
    expect(actual.timeWindow).toBe(expected)
  })
})
