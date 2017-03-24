import reducer from 'src/dashboards/reducers/ui'
import timeRanges from 'hson!src/shared/data/timeRanges.hson';

import {
  loadDashboards,
  setDashboard,
  setTimeRange,
  setEditMode,
  updateDashboardCells,
  editDashboardCell,
  renameDashboardCell,
  syncDashboardCell,
} from 'src/dashboards/actions'

const noopAction = () => {
  return {type: 'NOOP'}
}

let state = undefined
const timeRange = timeRanges[1];
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
      dashboard: d1,
    }

    expect(actual.dashboards).to.deep.equal(expected.dashboards)
    expect(actual.dashboard).to.deep.equal(expected.dashboard)
  })

  it('can set a dashboard', () => {
    const loadedState = reducer(state, loadDashboards(dashboards, d1.id))
    const actual = reducer(loadedState, setDashboard(d2.id))

    expect(actual.dashboard).to.deep.equal(d2)
  })

  it('can set the time range', () => {
    const expected = {upper: null, lower: 'now() - 1h'}
    const actual = reducer(state, setTimeRange(expected))

    expect(actual.timeRange).to.deep.equal(expected)
  })

  it('can set edit mode', () => {
    const isEditMode = true
    const actual = reducer(state, setEditMode(isEditMode))
    expect(actual.isEditMode).to.equal(isEditMode)
  })

  it('can update dashboard cells', () => {
    state = {
      dashboard: d1,
      dashboards,
    }

    const cells = [{id: 1}, {id: 2}]

    const expected = {
      id: 1,
      cells,
      name: 'd1',
    }

    const actual = reducer(state, updateDashboardCells(cells))

    expect(actual.dashboard).to.deep.equal(expected)
    expect(actual.dashboards[0]).to.deep.equal(expected)
  })

  it('can edit cell', () => {
    const dash = {...d1, cells}
    state = {
      dashboard: dash,
      dashboards: [dash],
    }

    const actual = reducer(state, editDashboardCell(0, 0, true))
    expect(actual.dashboards[0].cells[0].isEditing).to.equal(true)
    expect(actual.dashboard.cells[0].isEditing).to.equal(true)
  })

  it('can sync a cell', () => {
    const newCellName = 'watts is kinda cool'
    const newCell = {
      x: c1.x,
      y: c1.y,
      name: newCellName
    }
    const dash = {...d1, cells: [c1]}
    state = {
      dashboard: dash,
      dashboards: [dash],
    }

    const actual = reducer(state, syncDashboardCell(newCell))
    expect(actual.dashboards[0].cells[0].name).to.equal(newCellName)
    expect(actual.dashboard.cells[0].name).to.equal(newCellName)
  })

  it('can rename cells', () => {
    const c2 = {...c1, isEditing: true}
    const dash = {...d1, cells: [c2]}
    state = {
      dashboard: dash,
      dashboards: [dash],
    }

    const actual = reducer(state, renameDashboardCell(0, 0, "Plutonium Consumption Rate (ug/sec)"))
    expect(actual.dashboards[0].cells[0].name).to.equal("Plutonium Consumption Rate (ug/sec)")
    expect(actual.dashboard.cells[0].name).to.equal("Plutonium Consumption Rate (ug/sec)")
  })
})
