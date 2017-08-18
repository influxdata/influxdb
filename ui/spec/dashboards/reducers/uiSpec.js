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
  templateVariableSelected,
  templateVariablesSelectedByName,
  cancelEditCell,
} from 'src/dashboards/actions'

let state
const templates = [
  {
    id: '1',
    type: 'query',
    label: 'test query',
    tempVar: ':region:',
    query: {
      db: 'db1',
      rp: 'rp1',
      measurement: 'm1',
      influxql: 'SHOW TAGS WHERE CHRONOGIRAFFE = "friend"',
    },
    values: [
      {value: 'us-west', type: 'tagKey', selected: false},
      {value: 'us-east', type: 'tagKey', selected: true},
      {value: 'us-mount', type: 'tagKey', selected: false},
    ],
  },
  {
    id: '2',
    type: 'csv',
    label: 'test csv',
    tempVar: ':temperature:',
    values: [
      {value: '98.7', type: 'measurement', selected: false},
      {value: '99.1', type: 'measurement', selected: false},
      {value: '101.3', type: 'measurement', selected: true},
    ],
  },
]

const d1 = {
  id: 1,
  cells: [],
  name: 'd1',
  templates,
}
const d2 = {id: 2, cells: [], name: 'd2', templates: []}
const dashboards = [d1, d2]
const c1 = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  id: 1,
  i: 'im-a-cell-id-index',
  isEditing: false,
  name: 'Gigawatts',
}

const editingCell = {
  i: 1,
  isEditing: true,
  name: 'Edit me',
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
      templates,
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

    const actual = reducer(
      state,
      renameDashboardCell(dash, 0, 0, 'Plutonium Consumption Rate (ug/sec)')
    )
    expect(actual.dashboards[0].cells[0].name).to.equal(
      'Plutonium Consumption Rate (ug/sec)'
    )
  })

  it('can select a different template variable', () => {
    const dash = _.cloneDeep(d1)
    state = {
      dashboards: [dash],
    }

    const value = dash.templates[0].values[2].value
    const actual = reducer(
      state,
      templateVariableSelected(dash.id, dash.templates[0].id, [{value}])
    )

    expect(actual.dashboards[0].templates[0].values[0].selected).to.equal(false)
    expect(actual.dashboards[0].templates[0].values[1].selected).to.equal(false)
    expect(actual.dashboards[0].templates[0].values[2].selected).to.equal(true)
  })

  it('can select template variable values by name', () => {
    const dash = _.cloneDeep(d1)
    state = {
      dashboards: [dash],
    }

    const selected = {region: 'us-west', temperature: '99.1'}
    const actual = reducer(
      state,
      templateVariablesSelectedByName(dash.id, selected)
    )

    console.log(JSON.stringify(actual, null, 2))

    expect(actual.dashboards[0].templates[0].values[0].selected).to.equal(true)
    expect(actual.dashboards[0].templates[0].values[1].selected).to.equal(false)
    expect(actual.dashboards[0].templates[0].values[2].selected).to.equal(false)
    expect(actual.dashboards[0].templates[1].values[0].selected).to.equal(false)
    expect(actual.dashboards[0].templates[1].values[1].selected).to.equal(true)
    expect(actual.dashboards[0].templates[1].values[2].selected).to.equal(false)
  })

  it('can cancel cell editing', () => {
    const dash = _.cloneDeep(d1)
    dash.cells = [editingCell]

    const actual = reducer(
      {dashboards: [dash]},
      cancelEditCell(dash.id, editingCell.i)
    )

    expect(actual.dashboards[0].cells[0].isEditing).to.equal(false)
    expect(actual.dashboards[0].cells[0].name).to.equal(editingCell.name)
  })
})
