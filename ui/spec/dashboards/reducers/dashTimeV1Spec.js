import reducer from 'src/dashboards/reducers/dashTimeV1'
import {setDashTimeV1} from 'src/dashboards/actions/index'

describe.only('Dashboards.Reducers.DashTimeV1', () => {
  it('can load initial state', () => {
    const noopAction = () => ({type: 'NOOP'})
    const actual = reducer(undefined, noopAction)
    const expected = {dashTimeV1: []}

    expect(actual).to.deep.equal(expected)
  })

  it('can set a dashboard time', () => {
    const dashboardID = 1
    const timeRange = {upper: null, lower: 'now() - 15m'}

    const actual = reducer(undefined, setDashTimeV1(dashboardID, timeRange))
    const expected = [{dashboardID, timeRange}]

    expect(actual.dashTimeV1).to.deep.equal(expected)
  })
})
