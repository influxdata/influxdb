import reducer from 'src/dashboards/reducers/ui'
import timeRanges from 'hson!src/shared/data/timeRanges.hson';

import {
  loadDashboards,
  setDashboard,
  setTimeRange,
} from 'src/dashboards/actions'

const noopAction = () => {
  return {type: 'NOOP'}
}

let state = undefined
const timeRange = timeRanges[1];
const d1 = {id: 1, cells: [], name: "d1"}
const d2 = {id: 2, cells: [], name: "d2"}
const dashboards = [d1, d2]

describe('DataExplorer.Reducers.UI', () => {
  it('can load the dashboards', () => {
    const actual = reducer(state, loadDashboards(dashboards, d1.id))
    const expected = {
      dashboards,
      dashboard: d1,
      timeRange,
    }

    expect(actual).to.deep.equal(expected)
  })

  it('can set a dashboard', () => {
    const loadedState = reducer(state, loadDashboards(dashboards, d1.id))
    const actual = reducer(loadedState, setDashboard(d2.id))
    const expected = {
      dashboards,
      dashboard: d2,
      timeRange,
    }

    expect(actual).to.deep.equal(expected)
  })

  it('can set the time range', () => {
    const expected = {upper: null, lower: 'now() - 1h'}
    const actual = reducer(state, setTimeRange(expected))

    expect(actual.timeRange).to.deep.equal(expected)
  })
})
