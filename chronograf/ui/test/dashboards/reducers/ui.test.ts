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
  templateVariableLocalSelected,
  setActiveCell,
  updateTemplates,
} from 'src/dashboards/actions'

let state

const t2 = {
  ...template,
  id: '2',
  type: TemplateType.CSV,
  label: 'test csv',
  tempVar: ':temperature:',
  values: [
    {
      value: '98.7',
      type: TemplateValueType.Measurement,
      selected: false,
      localSelected: true,
    },
    {
      value: '99.1',
      type: TemplateValueType.Measurement,
      selected: false,
      localSelected: false,
    },
    {
      value: '101.3',
      type: TemplateValueType.Measurement,
      selected: true,
      localSelected: false,
    },
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

  it('can pick a different template variable', () => {
    const dash = _.cloneDeep(d1)
    state = {
      dashboards: [dash],
    }

    const action = templateVariableLocalSelected(
      dash.id,
      dash.templates[0].id,
      dash.templates[0].values[2]
    )

    const actual = reducer(state, action).dashboards[0].templates[0].values

    expect(actual[0].localSelected).toBe(false)
    expect(actual[1].localSelected).toBe(false)
    expect(actual[2].localSelected).toBe(true)
  })

  describe('SET_ACTIVE_CELL', () => {
    it('can set the active cell', () => {
      const activeCellID = '1'
      const actual = reducer(initialState, setActiveCell(activeCellID))

      expect(actual.activeCellID).toEqual(activeCellID)
    })
  })

  describe('UPDATE_TEMPLATE_VARIABLES', () => {
    it('can update template variables', () => {
      const thisState = {
        ...initialState,
        dashboards: [
          {
            ...dashboard,
            templates: [
              {
                id: '0',
                tempVar: ':foo:',
                label: '',
                type: TemplateType.CSV,
                values: [],
              },
              {
                id: '1',
                tempVar: ':bar:',
                label: '',
                type: TemplateType.CSV,
                values: [],
              },
              {
                id: '2',
                tempVar: ':baz:',
                label: '',
                type: TemplateType.CSV,
                values: [],
              },
            ],
          },
        ],
      }

      const newTemplates = [
        {
          id: '0',
          tempVar: ':foo:',
          label: '',
          type: TemplateType.CSV,
          values: [
            {
              type: TemplateValueType.CSV,
              value: '',
              selected: true,
              localSelected: true,
            },
          ],
        },
        {
          id: '1',
          tempVar: ':bar:',
          label: '',
          type: TemplateType.CSV,
          values: [
            {
              type: TemplateValueType.CSV,
              value: '',
              selected: false,
              localSelected: false,
            },
          ],
        },
      ]

      const result = reducer(thisState, updateTemplates(newTemplates))

      // Variables present in payload are updated
      expect(result.dashboards[0].templates).toContainEqual(newTemplates[0])
      expect(result.dashboards[0].templates).toContainEqual(newTemplates[1])

      // Variables not present in action payload are left untouched
      expect(result.dashboards[0].templates).toContainEqual(
        thisState.dashboards[0].templates[2]
      )
    })
  })
})
