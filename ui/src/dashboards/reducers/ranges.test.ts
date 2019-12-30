import reducer from 'src/dashboards/reducers/ranges'

import {
  setDashboardTimeRange,
  deleteTimeRange,
} from 'src/dashboards/actions/ranges'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {pastHourTimeRange} from 'src/shared/constants/timeRanges'

const emptyState = {}
const dashboardID = '1'

describe('Dashboards.Reducers.Ranges', () => {
  it('can delete a dashboard time range', () => {
    const state: RangeState = {[dashboardID]: pastHourTimeRange}

    const actual = reducer(state, deleteTimeRange(dashboardID))
    const expected = emptyState

    expect(actual).toEqual(expected)
  })

  describe('setting a dashboard time range', () => {
    it('can update an existing dashboard', () => {
      const state: RangeState = {[dashboardID]: pastHourTimeRange}

      const timeRange = {
        type: 'custom' as 'custom',
        upper: '2017-10-07 12:05',
        lower: '2017-10-05 12:04',
      }

      const actual = reducer(
        state,
        setDashboardTimeRange(dashboardID, timeRange)
      )
      const expected = {[dashboardID]: timeRange}

      expect(actual).toEqual(expected)
    })

    it('can set a new time range if none exists', () => {
      const actual = reducer(
        emptyState,
        setDashboardTimeRange(dashboardID, pastHourTimeRange)
      )

      const expected = {[dashboardID]: pastHourTimeRange}

      expect(actual).toEqual(expected)
    })
  })
})
