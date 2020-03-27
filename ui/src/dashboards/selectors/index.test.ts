// Funcs
import {getTimeRange} from 'src/dashboards/selectors/index'

// Types
import {RangeState} from 'src/dashboards/reducers/ranges'
import {TimeRange} from 'src/types'

// Constants
import {
  DEFAULT_TIME_RANGE,
  pastFifteenMinTimeRange,
  pastHourTimeRange,
} from 'src/shared/constants/timeRanges'

const untypedGetTimeRangeByDashboardID = getTimeRange as (
  a: {ranges: RangeState},
  dashID: string
) => TimeRange

describe('Dashboards.Selector', () => {
  const dashboardIDs = ['04c6f3976f4b8001', '04c6f3976f4b8000']
  const ranges: RangeState = {
    [dashboardIDs[0]]: pastFifteenMinTimeRange,
    [dashboardIDs[1]]: pastHourTimeRange,
  }

  it('should return the the correct range when a matching dashboard ID is found', () => {
    expect(untypedGetTimeRangeByDashboardID({ranges}, dashboardIDs[0])).toEqual(
      pastFifteenMinTimeRange
    )
  })

  it('should return the the default range when no matching dashboard ID is found', () => {
    expect(untypedGetTimeRangeByDashboardID({ranges}, 'Oogum Boogum')).toEqual(
      DEFAULT_TIME_RANGE
    )
  })

  it('should return the the default range when no ranges are passed in', () => {
    expect(
      untypedGetTimeRangeByDashboardID({ranges: {}}, dashboardIDs[0])
    ).toEqual(DEFAULT_TIME_RANGE)
  })
})
