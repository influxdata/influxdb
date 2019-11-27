// Funcs
import {getTimeRangeByDashboardID} from 'src/dashboards/selectors/index'

// Types
import {Range} from 'src/dashboards/reducers/ranges'

// Constants
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

describe('Dashboards.Selector', () => {
  const ranges: Range[] = [
    {
      dashboardID: '04c6f3976f4b8001',
      lower: 'now() - 5m',
      upper: null,
    },
    {
      dashboardID: '04c6f3976f4b8000',
      lower: '2019-11-07T10:46:51.000Z',
      upper: '2019-11-28T22:46:51.000Z',
    },
  ]
  const dashboardID: string = '04c6f3976f4b8000'
  it('should return the default timerange when a no data is passed', () => {
    expect(getTimeRangeByDashboardID()).toEqual(DEFAULT_TIME_RANGE)
  })
  it('should return the the correct range when a matching dashboard ID is found', () => {
    expect(getTimeRangeByDashboardID(ranges, dashboardID)).toEqual(ranges[1])
  })
  it('should return the the default range when no matching dashboard ID is found', () => {
    expect(getTimeRangeByDashboardID(ranges, 'Oogum Boogum')).toEqual(
      DEFAULT_TIME_RANGE
    )
  })
  it('should return the the default range when no ranges are passed in', () => {
    expect(getTimeRangeByDashboardID([], dashboardID)).toEqual(
      DEFAULT_TIME_RANGE
    )
  })
})
