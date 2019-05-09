import {autoRefreshReducer, initialState} from 'src/shared/reducers/autoRefresh'

import {
  setAutoRefreshInterval,
  setAutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'
import {AutoRefreshStatus} from 'src/types'

describe('autoRefresh reducer', () => {
  it('can set interval with empty state', () => {
    const interval = 1000
    const actual = autoRefreshReducer(
      initialState(),
      setAutoRefreshInterval('dashID', interval)
    )

    const expected = {['dashID']: {...AUTOREFRESH_DEFAULT, interval}}

    expect(actual).toEqual(expected)
  })

  it('can set interval ', () => {
    const interval = 5000
    const dashboardID = 'anotherDash'
    const startingAutoRefresh = {
      interval: 1000,
      status: AutoRefreshStatus.Active,
    }
    const startingState = {
      [dashboardID]: startingAutoRefresh,
      ['dash']: AUTOREFRESH_DEFAULT,
    }
    const actual = autoRefreshReducer(
      startingState,
      setAutoRefreshInterval(dashboardID, interval)
    )

    const expected = {
      [dashboardID]: {...startingAutoRefresh, interval},
      ['dash']: AUTOREFRESH_DEFAULT,
    }

    expect(actual).toEqual(expected)
  })

  it('can set status with empty state', () => {
    const status = AutoRefreshStatus.Active
    const actual = autoRefreshReducer(
      initialState(),
      setAutoRefreshStatus('dashID', status)
    )

    const expected = {['dashID']: {...AUTOREFRESH_DEFAULT, status}}

    expect(actual).toEqual(expected)
  })

  it('can set status ', () => {
    const status = AutoRefreshStatus.Disabled
    const dashboardID = 'anotherDash'
    const startingAutoRefresh = {
      interval: 1000,
      status: AutoRefreshStatus.Active,
    }
    const startingState = {
      [dashboardID]: startingAutoRefresh,
      ['dash']: AUTOREFRESH_DEFAULT,
    }
    const actual = autoRefreshReducer(
      startingState,
      setAutoRefreshStatus(dashboardID, status)
    )

    const expected = {
      [dashboardID]: {...startingAutoRefresh, status},
      ['dash']: AUTOREFRESH_DEFAULT,
    }

    expect(actual).toEqual(expected)
  })
})
