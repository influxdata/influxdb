// Funcs
import {
  getTimeRange,
  getTimeRangeWithTimezone,
} from 'src/dashboards/selectors/index'
import moment from 'moment'

// Types
import {RangeState} from 'src/dashboards/reducers/ranges'
import {CurrentDashboardState} from 'src/shared/reducers/currentDashboard'
import {TimeRange, TimeZone, CustomTimeRange} from 'src/types'
import {AppState as AppPresentationState} from 'src/shared/reducers/app'

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

const untypedGetTimeRangeWithTimeZone = getTimeRangeWithTimezone as (a: {
  ranges: RangeState
  currentDashboard: CurrentDashboardState
  app: AppPresentationState
}) => TimeRange

describe('Dashboards.Selector', () => {
  const dashboardIDs = [
    '04c6f3976f4b8001',
    '04c6f3976f4b8000',
    '04c6f3976f4b8002',
  ]
  const lower = `2020-05-05T10:00:00${moment().format('Z')}`
  const upper = `2020-05-05T11:00:00${moment().format('Z')}`
  const customTimeRange = {
    lower,
    upper,
    type: 'custom',
  } as CustomTimeRange
  const ranges: RangeState = {
    [dashboardIDs[0]]: pastFifteenMinTimeRange,
    [dashboardIDs[1]]: pastHourTimeRange,
    [dashboardIDs[2]]: customTimeRange,
  }

  it('should return the correct range when a matching dashboard ID is found', () => {
    const currentDashboard = {id: dashboardIDs[0]}

    expect(
      untypedGetTimeRangeByDashboardID({ranges, currentDashboard})
    ).toEqual(pastFifteenMinTimeRange)
  })

  it('should return the default range when no matching dashboard ID is found', () => {
    const currentDashboard = {id: 'Oogum Boogum'}

    expect(
      untypedGetTimeRangeByDashboardID({ranges, currentDashboard})
    ).toEqual(DEFAULT_TIME_RANGE)
  })

  it('should return the default range when no ranges are passed in', () => {
    const currentDashboard = {id: dashboardIDs[0]}

    expect(
      untypedGetTimeRangeByDashboardID({ranges: {}, currentDashboard})
    ).toEqual(DEFAULT_TIME_RANGE)
  })

  it('should return the an unmodified version of the timeRange when the timeZone is local', () => {
    const currentDashboard = {id: dashboardIDs[2]}
    const app: AppPresentationState = {
      ephemeral: {
        inPresentationMode: false,
      },
      persisted: {
        autoRefresh: 0,
        showTemplateControlBar: false,
        navBarState: 'expanded',
        timeZone: 'Local' as TimeZone,
        theme: 'dark',
      },
    }

    expect(
      untypedGetTimeRangeWithTimeZone({ranges, currentDashboard, app})
    ).toEqual(customTimeRange)
  })

  it('should return the timeRange for the same hour with a UTC timezone when the timeZone is UTC', () => {
    const currentDashboard = {id: dashboardIDs[2]}

    const app: AppPresentationState = {
      ephemeral: {
        inPresentationMode: false,
      },
      persisted: {
        autoRefresh: 0,
        showTemplateControlBar: false,
        navBarState: 'expanded',
        timeZone: 'UTC' as TimeZone,
        theme: 'dark',
      },
    }

    const expected = {
      lower: `2020-05-05T10:00:00Z`,
      upper: `2020-05-05T11:00:00Z`,
      type: 'custom',
    }

    expect(
      untypedGetTimeRangeWithTimeZone({ranges, currentDashboard, app})
    ).toEqual(expected)
  })
})
