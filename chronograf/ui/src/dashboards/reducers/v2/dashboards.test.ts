// Reducer
import reducer from 'src/dashboards/reducers/v2/dashboards'

// Actions
import {
  loadDashboard,
  loadDashboards,
  deleteDashboard,
  updateDashboard,
  deleteCell,
} from 'src/dashboards/actions/v2/'

// Resources
import {dashboard} from 'src/dashboards/resources'

describe('dashboards reducer', () => {
  it('can load the dashboards', () => {
    const expected = [dashboard]
    const actual = reducer([], loadDashboards(expected))

    expect(actual).toEqual(expected)
  })

  it('can delete a dashboard', () => {
    const d2 = {...dashboard, id: '2'}
    const state = [dashboard, d2]
    const expected = [dashboard]
    const actual = reducer(state, deleteDashboard(d2.id))

    expect(actual).toEqual(expected)
  })

  it('can load a dashboard', () => {
    const loadedDashboard = {...dashboard, name: 'updated'}
    const d2 = {...dashboard, id: '2'}
    const state = [dashboard, d2]

    const expected = [loadedDashboard, d2]
    const actual = reducer(state, loadDashboard(loadedDashboard))

    expect(actual).toEqual(expected)
  })

  it('can update a dashboard', () => {
    const updates = {...dashboard, name: 'updated dash'}
    const expected = [updates]
    const actual = reducer([dashboard], updateDashboard(updates))

    expect(actual).toEqual(expected)
  })

  it('can delete a cell from a dashboard', () => {
    const expected = [{...dashboard, cells: []}]
    const actual = reducer(
      [dashboard],
      deleteCell(dashboard, dashboard.cells[0])
    )

    expect(actual).toEqual(expected)
  })
})
