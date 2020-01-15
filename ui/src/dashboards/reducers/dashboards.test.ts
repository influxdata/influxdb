// Libraries
import {normalize} from 'normalizr'

// Schema
import * as schemas from 'src/schemas'

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
} from 'src/dashboards/actions/creators'

// Resources
import {dashboard} from 'src/dashboards/resources'
import {labels} from 'mocks/dummyData'

// Types
import {RemoteDataState, DashboardEntities, Dashboard} from 'src/types'

const status = RemoteDataState.Done

const initialState = () => ({
  status,
  byID: {
    [dashboard.id]: dashboard,
    ['2']: {...dashboard, id: '2'},
  },
  allIDs: [dashboard.id, '2'],
})

describe('dashboards reducer', () => {
  it('can set the dashboards', () => {
    const schema = normalize<Dashboard, DashboardEntities, string[]>(
      [dashboard],
      schemas.arrayOfDashboards
    )

    const byID = schema.entities.dashboards
    const allIDs = schema.result

    const actual = reducer(undefined, setDashboards(status, schema))

    expect(actual.byID).toEqual(byID)
    expect(actual.allIDs).toEqual(allIDs)
  })

  it('can remove a dashboard', () => {
    const allIDs = [dashboard.id]
    const byID = {[dashboard.id]: dashboard}

    const state = initialState()
    const expected = {status, byID, allIDs}
    const actual = reducer(state, removeDashboard(state.allIDs[1]))

    expect(actual).toEqual(expected)
  })

  it('can set a dashboard', () => {
    const name = 'updated name'
    const loadedDashboard = {...dashboard, name: 'updated name'}
    const schema = normalize<Dashboard, DashboardEntities, string>(
      loadedDashboard,
      schemas.dashboard
    )

    const state = initialState()

    const actual = reducer(state, setDashboard(schema))

    expect(actual.byID[dashboard.id].name).toEqual(name)
  })

  it('can edit a dashboard', () => {
    const name = 'updated name'
    const updates = {...dashboard, name}

    const schema = normalize<Dashboard, DashboardEntities, string>(
      updates,
      schemas.dashboard
    )

    const state = initialState()
    const actual = reducer(state, editDashboard(schema))

    expect(actual.byID[dashboard.id].name).toEqual(name)
  })

  it('can remove a cell from a dashboard', () => {
    const state = initialState()
    const {id} = dashboard
    const cellID = dashboard.cells[0].id
    const actual = reducer(state, removeCell(id, cellID))

    expect(actual.byID[id].cells).toEqual([])
  })

  it('can add labels to a dashboard', () => {
    const {id} = dashboard
    const state = initialState()
    const label = labels[0]

    const actual = reducer(state, addDashboardLabel(id, label))

    expect(actual.byID[id].labels).toEqual([label])
  })

  it('can remove labels from a dashboard', () => {
    const {id} = dashboard
    const label = labels[0]

    const state = initialState()
    const withLabel = reducer(state, addDashboardLabel(id, label))
    const actual = reducer(withLabel, removeDashboardLabel(id, labels[0].id))

    expect(actual.byID[id].labels).toEqual([])
  })
})
