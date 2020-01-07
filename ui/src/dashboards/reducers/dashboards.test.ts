// Reducer
import {dashboardsReducer as reducer} from 'src/dashboards/reducers/dashboards'

// Actions
import {
  setDashboard,
  setDashboards,
  removeDashboard,
  editDashboard,
  removeCell,
  addDashboardLabel,
  removeDashboardLabel,
} from 'src/dashboards/actions/'

// Resources
import {dashboard} from 'src/dashboards/resources'
import {labels} from 'mocks/dummyData'
import {RemoteDataState} from '@influxdata/clockface'

const status = RemoteDataState.Done

describe('dashboards reducer', () => {
  it('can set the dashboards', () => {
    const list = [dashboard]

    const expected = {status, list}
    const actual = reducer(undefined, setDashboards(status, list))

    expect(actual).toEqual(expected)
  })

  it('can remove a dashboard', () => {
    const d2 = {...dashboard, id: '2'}
    const list = [dashboard, d2]
    const expected = {list: [dashboard], status}
    const actual = reducer({list, status}, removeDashboard(d2.id))

    expect(actual).toEqual(expected)
  })

  it('can set a dashboard', () => {
    const loadedDashboard = {...dashboard, name: 'updated'}
    const d2 = {...dashboard, id: '2'}
    const state = {status, list: [dashboard, d2]}

    const expected = {status, list: [loadedDashboard, d2]}
    const actual = reducer(state, setDashboard(loadedDashboard))

    expect(actual).toEqual(expected)
  })

  it('can edit a dashboard', () => {
    const updates = {...dashboard, name: 'updated dash'}
    const expected = {status, list: [updates]}
    const actual = reducer({status, list: [dashboard]}, editDashboard(updates))

    expect(actual).toEqual(expected)
  })

  it('can remove a cell from a dashboard', () => {
    const expected = {status, list: [{...dashboard, cells: []}]}
    const actual = reducer(
      {status, list: [dashboard]},
      removeCell(dashboard, dashboard.cells[0])
    )

    expect(actual).toEqual(expected)
  })

  it('can add labels to a dashboard', () => {
    const dashboardWithoutLabels = {...dashboard, labels: []}
    const expected = {status, list: [{...dashboard, labels: [labels[0]]}]}
    const actual = reducer(
      {status, list: [dashboardWithoutLabels]},
      addDashboardLabel(dashboardWithoutLabels.id, labels[0])
    )

    expect(actual).toEqual(expected)
  })

  it('can remove labels from a dashboard', () => {
    const leftOverLabel = {...labels[0], name: 'wowowowo', id: '3'}
    const dashboardWithLabels = {
      ...dashboard,
      labels: [labels[0], leftOverLabel],
    }
    const expected = {status, list: [{...dashboard, labels: [leftOverLabel]}]}
    const actual = reducer(
      {status, list: [dashboardWithLabels]},
      removeDashboardLabel(dashboardWithLabels.id, labels[0])
    )

    expect(actual).toEqual(expected)
  })
})
