import reducer, {defaultState} from 'src/logs/reducers'
import {setTimeWindow} from 'src/logs/actions'

describe('Logs.Reducers', () => {
  it('can set a time window', () => {
    const expected = {
      timeOption: 'now',
      windowOption: '1h',
      upper: null,
      lower: 'now() - 1h',
      seconds: 3600,
    }

    const actual = reducer(defaultState, setTimeWindow(expected))
    expect(actual.timeWindow).toBe(expected)
  })
})
