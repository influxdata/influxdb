// Funcs
import {getTimeRange} from 'src/dashboards/selectors/index'

// Types
import {RangeState} from 'src/dashboards/reducers/ranges'
import {CurrentDashboardState} from 'src/shared/reducers/currentDashboard'
import {TimeRange} from 'src/types'

// Constants
import {
  DEFAULT_TIME_RANGE,
  pastFifteenMinTimeRange,
  pastHourTimeRange,
} from 'src/shared/constants/timeRanges'

const untypedGetTimeRangeByDashboardID = getTimeRange as (a: {
  ranges: RangeState
  currentDashboard: CurrentDashboardState
}) => TimeRange

describe('Dashboards.Selector', () => {
  const dashboardIDs = ['04c6f3976f4b8001', '04c6f3976f4b8000']
  const ranges: RangeState = {
    [dashboardIDs[0]]: pastFifteenMinTimeRange,
    [dashboardIDs[1]]: pastHourTimeRange,
  }

  it('should return the the correct range when a matching dashboard ID is found', () => {
    const currentDashboard = {id: dashboardIDs[0]}

    expect(
      untypedGetTimeRangeByDashboardID({ranges, currentDashboard})
    ).toEqual(pastFifteenMinTimeRange)
  })

  it('should return the the default range when no matching dashboard ID is found', () => {
    const currentDashboard = {id: 'Oogum Boogum'}

    expect(
      untypedGetTimeRangeByDashboardID({ranges, currentDashboard})
    ).toEqual(DEFAULT_TIME_RANGE)
  })

  it('should return the the default range when no ranges are passed in', () => {
    const currentDashboard = {id: dashboardIDs[0]}

    expect(
      untypedGetTimeRangeByDashboardID({ranges: {}, currentDashboard})
    ).toEqual(DEFAULT_TIME_RANGE)
  })
})
