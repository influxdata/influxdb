import reducer from 'src/dashboards/reducers/dashTimeV1'
import {
  addDashTimeV1,
  updateDashTimeV1,
  deleteDashboard,
} from 'src/dashboards/actions/index'

const initialState = {
  ranges: [],
}

const emptyState = undefined
const dashboardID = 1
const timeRange = {upper: null, lower: 'now() - 15m'}

describe('Dashboards.Reducers.DashTimeV1', () => {
  it('can load initial state', () => {
    const noopAction = () => ({type: 'NOOP'})
    const actual = reducer(emptyState, noopAction)
    const expected = {ranges: []}

    expect(actual).to.deep.equal(expected)
  })

  it('can add a dashboard time', () => {
    const actual = reducer(emptyState, addDashTimeV1(dashboardID, timeRange))
    const expected = [{dashboardID, timeRange}]

    expect(actual.ranges).to.deep.equal(expected)
  })

  it('can delete a dashboard time range', () => {
    const state = {
      ranges: [{dashboardID, timeRange}],
    }

    const actual = reducer(state, deleteDashboard({}, dashboardID))
    const expected = []

    expect(actual.ranges).to.deep.equal(expected)
  })

  it('can updated a dashboard time range', () => {
    const state = {
      ranges: [{dashboardID, timeRange}],
    }

    const newTimeRange = {upper: '2017-10-07 12:05', lower: '2017-10-05 12:04'}

    const actual = reducer(state, updateDashTimeV1(dashboardID, newTimeRange))
    const expected = [{dashboardID, timeRange: newTimeRange}]

    expect(actual.ranges).to.deep.equal(expected)
  })
})
