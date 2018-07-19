import reducer, {defaultState} from 'src/logs/reducers'
import {setTimeWindow, setTimeMarker, setTimeBounds} from 'src/logs/actions'

describe('Logs.Reducers', () => {
  it('can set a time window', () => {
    const actionPayload = {
      windowOption: '10m',
      seconds: 600,
    }

    const expected = {
      timeOption: 'now',
      windowOption: '10m',
      upper: null,
      lower: 'now() - 1m',
      seconds: 600,
    }

    const actual = reducer(defaultState, setTimeWindow(actionPayload))
    expect(actual.timeRange).toEqual(expected)
  })

  it('can set a time marker', () => {
    const actionPayload = {
      timeOption: '2018-07-10T22:22:21.769Z',
    }

    const expected = {
      timeOption: '2018-07-10T22:22:21.769Z',
      windowOption: '1m',
      upper: null,
      lower: 'now() - 1m',
      seconds: 60,
    }

    const actual = reducer(defaultState, setTimeMarker(actionPayload))
    expect(actual.timeRange).toEqual(expected)
  })

  it('can set the time bounds', () => {
    const payload = {
      upper: '2018-07-10T22:20:21.769Z',
      lower: '2018-07-10T22:22:21.769Z',
    }

    const expected = {
      timeOption: 'now',
      windowOption: '1m',
      upper: '2018-07-10T22:20:21.769Z',
      lower: '2018-07-10T22:22:21.769Z',
      seconds: 60,
    }

    const actual = reducer(defaultState, setTimeBounds(payload))
    expect(actual.timeRange).toEqual(expected)
  })
})
