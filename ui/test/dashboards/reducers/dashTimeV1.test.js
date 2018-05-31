import reducer from 'src/dashboards/reducers/dashTimeV1'
import {setDashTimeV1, deleteDashboard} from 'src/dashboards/actions/index'

const emptyState = undefined
const dashboardID = 1
const timeRange = {upper: null, lower: 'now() - 15m'}

describe('Dashboards.Reducers.DashTimeV1', () => {
  it('can load initial state', () => {
    const noopAction = () => ({type: 'NOOP'})
    const actual = reducer(emptyState, noopAction)
    const expected = {ranges: []}

    expect(actual).toEqual(expected)
  })

  it('can delete a dashboard time range', () => {
    const state = {
      ranges: [{dashboardID, timeRange}],
    }
    const dashboard = {id: dashboardID}

    const actual = reducer(state, deleteDashboard(dashboard))
    const expected = []

    expect(actual.ranges).toEqual(expected)
  })

  describe('setting a dashboard time range', () => {
    it('can update an existing dashboard', () => {
      const state = {
        ranges: [{dashboardID, upper: timeRange.upper, lower: timeRange.lower}],
      }

      const {upper, lower} = {
        upper: '2017-10-07 12:05',
        lower: '2017-10-05 12:04',
      }

      const actual = reducer(state, setDashTimeV1(dashboardID, {upper, lower}))
      const expected = [{dashboardID, upper, lower}]

      expect(actual.ranges).toEqual(expected)
    })

    it('can set a new time range if none exists', () => {
      const actual = reducer(emptyState, setDashTimeV1(dashboardID, timeRange))

      const expected = [
        {dashboardID, upper: timeRange.upper, lower: timeRange.lower},
      ]

      expect(actual.ranges).toEqual(expected)
    })
  })
})
