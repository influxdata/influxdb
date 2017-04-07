import _ from 'lodash'

import reducer from 'src/dashboards/reducers/ui'

import {
  loadDashboards,
  deleteDashboardFailed,
  setTimeRange,
  updateDashboardCells,
  editDashboardCell,
  renameDashboardCell,
  syncDashboardCell,
} from 'src/dashboards/actions'

let state
const d1 = {id: 1, cells: [], name: "d1"}
const d2 = {id: 2, cells: [], name: "d2"}
const dashboards = [d1, d2]
const c1 = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  id: 1,
  isEditing: false,
  name: "Gigawatts",
}
const cells = [c1]

describe('DataExplorer.Reducers.UI', () => {
  it('can load the dashboards', () => {
    const actual = reducer(state, loadDashboards(dashboards, d1.id))
    const expected = {
      dashboards,
    }

    expect(actual.dashboards).to.deep.equal(expected.dashboards)
  })

  it('can handle a failed dashboard deletion', () => {
    const loadedState = reducer(state, loadDashboards([d1]))
    const actual = reducer(loadedState, deleteDashboardFailed(d2))
    const actualFirst = _.first(actual.dashboards)

    expect(actual.dashboards).to.have.length(2)
    _.forOwn(d2, (v, k) => {
      expect(actualFirst[k]).to.deep.equal(v)
    })
  })

  it('can set the time range', () => {
    const expected = {upper: null, lower: 'now() - 1h'}
    const actual = reducer(state, setTimeRange(expected))

    expect(actual.timeRange).to.deep.equal(expected)
  })

  it('can update dashboard cells', () => {
    state = {
      dashboards,
    }

    const updatedCells = [{id: 1}, {id: 2}]

    const expected = {
      id: 1,
      cells: updatedCells,
      name: 'd1',
    }

    const actual = reducer(state, updateDashboardCells(d1, updatedCells))

    expect(actual.dashboards[0]).to.deep.equal(expected)
  })

  it('can edit a cell', () => {
    const dash = {...d1, cells}
    state = {
      dashboards: [dash],
    }

    const actual = reducer(state, editDashboardCell(dash, 0, 0, true))
    expect(actual.dashboards[0].cells[0].isEditing).to.equal(true)
  })

  it('can sync a cell', () => {
    const newCellName = 'watts is kinda cool'
    const newCell = {
      x: c1.x,
      y: c1.y,
      name: newCellName,
    }
    const dash = {...d1, cells: [c1]}
    state = {
      dashboards: [dash],
    }

    const actual = reducer(state, syncDashboardCell(dash, newCell))
    expect(actual.dashboards[0].cells[0].name).to.equal(newCellName)
  })

  it('can rename cells', () => {
    const c2 = {...c1, isEditing: true}
    const dash = {...d1, cells: [c2]}
    state = {
      dashboards: [dash],
    }

    const actual = reducer(state, renameDashboardCell(dash, 0, 0, "Plutonium Consumption Rate (ug/sec)"))
    expect(actual.dashboards[0].cells[0].name).to.equal("Plutonium Consumption Rate (ug/sec)")
  })
})
