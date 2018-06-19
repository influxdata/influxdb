import _ from 'lodash'

import reducer from 'src/dashboards/reducers/ui'
import {template, dashboard, cell} from 'test/resources'
import {initialState} from 'src/dashboards/reducers/ui'
import {TemplateType, TemplateValueType} from 'src/types'

import {
  setTimeRange,
  loadDashboards,
  deleteDashboard,
  syncDashboardCell,
  deleteDashboardFailed,
  templateVariableSelected,
  editTemplateVariableValues,
  templateVariablesSelectedByName,
  setActiveCell,
} from 'src/dashboards/actions'

let state

const t2 = {
  ...template,
  id: '2',
  type: TemplateType.CSV,
  label: 'test csv',
  tempVar: ':temperature:',
  values: [
    {value: '98.7', type: TemplateValueType.Measurement, selected: false},
    {value: '99.1', type: TemplateValueType.Measurement, selected: false},
    {value: '101.3', type: TemplateValueType.Measurement, selected: true},
  ],
}

const templates = [template, t2]

const d1 = {
  ...dashboard,
  templates,
}

const d2 = {...dashboard, id: 2, cells: [], name: 'd2', templates: []}
const dashboards = [d1, d2]

describe('DataExplorer.Reducers.UI', () => {
  it('can load the dashboards', () => {
    const actual = reducer(state, loadDashboards(dashboards, d1.id))
    const expected = {
      dashboards,
    }

    expect(actual.dashboards).toEqual(expected.dashboards)
  })

  it('can delete a dashboard', () => {
    const actual = reducer({...initialState, dashboards}, deleteDashboard(d1))
    const expected = dashboards.filter(dash => dash.id !== d1.id)

    expect(actual.dashboards).toEqual(expected)
  })

  it('can handle a failed dashboard deletion', () => {
    const loadedState = reducer(state, loadDashboards([d1]))
    const actual = reducer(loadedState, deleteDashboardFailed(d2))
    const actualFirst = _.first(actual.dashboards)

    expect(actual.dashboards.length).toBe(2)
    _.forOwn(d2, (v, k) => {
      expect(actualFirst[k]).toEqual(v)
    })
  })

  it('can set the time range', () => {
    const expected = {upper: null, lower: 'now() - 1h'}
    const actual = reducer(state, setTimeRange(expected))

    expect(actual.timeRange).toEqual(expected)
  })

  it('can sync a cell', () => {
    const newCellName = 'watts is kinda cool'
    const newCell = {
      ...cell,
      name: newCellName,
    }

    const dash = {...d1, cells: [cell]}
    state = {
      dashboards: [dash],
    }

    const actual = reducer(state, syncDashboardCell(dash, newCell))
    expect(actual.dashboards[0].cells[0].name).toBe(newCellName)
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

    expect(actual.dashboards[0].templates[0].values[0].selected).toBe(false)
    expect(actual.dashboards[0].templates[0].values[1].selected).toBe(false)
    expect(actual.dashboards[0].templates[0].values[2].selected).toBe(true)
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

    expect(actual.dashboards[0].templates[0].values[0].selected).toBe(true)
    expect(actual.dashboards[0].templates[0].values[1].selected).toBe(false)
    expect(actual.dashboards[0].templates[0].values[2].selected).toBe(false)
    expect(actual.dashboards[0].templates[1].values[0].selected).toBe(false)
    expect(actual.dashboards[0].templates[1].values[1].selected).toBe(true)
    expect(actual.dashboards[0].templates[1].values[2].selected).toBe(false)
  })

  describe('SET_ACTIVE_CELL', () => {
    it('can set the active cell', () => {
      const activeCellID = '1'
      const actual = reducer(initialState, setActiveCell(activeCellID))

      expect(actual.activeCellID).toEqual(activeCellID)
    })
  })

  describe('EDIT_TEMPLATE_VARIABLE_VALUES', () => {
    it('can edit the tempvar values', () => {
      const actual = reducer(
        {...initialState, dashboards},
        editTemplateVariableValues(d1.id, template.id, ['v1', 'v2'])
      )

      const expected = [
        {
          selected: false,
          value: 'v1',
          type: 'tagKey',
        },
        {
          selected: false,
          value: 'v2',
          type: 'tagKey',
        },
      ]

      expect(actual.dashboards[0].templates[0].values).toEqual(expected)
    })

    it('can handle an empty template.values', () => {
      const ts = [{...template, values: []}]
      const ds = [{...d1, templates: ts}]

      const actual = reducer(
        {...initialState, dashboards: ds},
        editTemplateVariableValues(d1.id, template.id, ['v1', 'v2'])
      )

      const expected = [
        {
          selected: false,
          value: 'v1',
          type: 'tagKey',
        },
        {
          selected: false,
          value: 'v2',
          type: 'tagKey',
        },
      ]

      expect(actual.dashboards[0].templates[0].values).toEqual(expected)
    })
  })
})
