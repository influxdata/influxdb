import reducer from 'src/dashboards/reducers/v2/ranges'

import {setDashTimeV1, deleteTimeRange} from 'src/dashboards/actions/v2/ranges'

const emptyState = undefined
const dashboardID = '1'
const timeRange = {upper: null, lower: 'now() - 15m'}

describe('Dashboards.Reducers.Ranges', () => {
  it('can delete a dashboard time range', () => {
    const state = [{dashboardID, ...timeRange}]

    const actual = reducer(state, deleteTimeRange(dashboardID))
    const expected = []

    expect(actual).toEqual(expected)
  })

  describe('setting a dashboard time range', () => {
    it('can update an existing dashboard', () => {
      const state = [
        {dashboardID, upper: timeRange.upper, lower: timeRange.lower},
      ]

      const {upper, lower} = {
        upper: '2017-10-07 12:05',
        lower: '2017-10-05 12:04',
      }

      const actual = reducer(state, setDashTimeV1(dashboardID, {upper, lower}))
      const expected = [{dashboardID, upper, lower}]

      expect(actual).toEqual(expected)
    })

    it('can set a new time range if none exists', () => {
      const actual = reducer(emptyState, setDashTimeV1(dashboardID, timeRange))

      const expected = [
        {dashboardID, upper: timeRange.upper, lower: timeRange.lower},
      ]

      expect(actual).toEqual(expected)
    })
  })
})
