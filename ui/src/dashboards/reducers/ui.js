import _ from 'lodash'
import timeRanges from 'hson!shared/data/timeRanges.hson'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 1h')

const initialState = {
  dashboards: [],
  timeRange: {lower, upper},
  isEditMode: false,
  cellQueryStatus: {queryID: null, status: null},
}

import {TEMPLATE_VARIABLE_SELECTED} from 'shared/constants/actionTypes'
import {TEMPLATE_VARIABLE_TYPES} from 'src/dashboards/constants'

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'LOAD_DASHBOARDS': {
      const {dashboards} = action.payload
      const newState = {
        dashboards,
      }

      return {...state, ...newState}
    }

    case 'SET_DASHBOARD_TIME_RANGE': {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    case 'UPDATE_DASHBOARD': {
      const {dashboard} = action.payload
      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? dashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'DELETE_DASHBOARD': {
      const {dashboard} = action.payload
      const newState = {
        dashboards: state.dashboards.filter(d => d.id !== dashboard.id),
      }

      return {...state, ...newState}
    }

    case 'DELETE_DASHBOARD_FAILED': {
      const {dashboard} = action.payload
      const newState = {
        dashboards: [_.cloneDeep(dashboard), ...state.dashboards],
      }
      return {...state, ...newState}
    }

    case 'UPDATE_DASHBOARD_CELLS': {
      const {cells, dashboard} = action.payload

      const newDashboard = {
        ...dashboard,
        cells,
      }

      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? newDashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'ADD_DASHBOARD_CELL': {
      const {cell, dashboard} = action.payload
      const {dashboards} = state

      const newCells = [cell, ...dashboard.cells]
      const newDashboard = {...dashboard, cells: newCells}
      const newDashboards = dashboards.map(
        d => (d.id === dashboard.id ? newDashboard : d)
      )
      const newState = {dashboards: newDashboards}

      return {...state, ...newState}
    }

    case 'EDIT_DASHBOARD_CELL': {
      const {x, y, isEditing, dashboard} = action.payload

      const cell = dashboard.cells.find(c => c.x === x && c.y === y)

      const newCell = {
        ...cell,
        isEditing,
      }

      const newDashboard = {
        ...dashboard,
        cells: dashboard.cells.map(c => (c.x === x && c.y === y ? newCell : c)),
      }

      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? newDashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'DELETE_DASHBOARD_CELL': {
      const {dashboard, cell} = action.payload

      const newCells = dashboard.cells.filter(
        c => !(c.x === cell.x && c.y === cell.y)
      )
      const newDashboard = {
        ...dashboard,
        cells: newCells,
      }
      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? newDashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'SYNC_DASHBOARD_CELL': {
      const {cell, dashboard} = action.payload

      const newDashboard = {
        ...dashboard,
        cells: dashboard.cells.map(
          c => (c.x === cell.x && c.y === cell.y ? cell : c)
        ),
      }

      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? newDashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'RENAME_DASHBOARD_CELL': {
      const {x, y, name, dashboard} = action.payload

      const cell = dashboard.cells.find(c => c.x === x && c.y === y)

      const newCell = {
        ...cell,
        name,
      }

      const newDashboard = {
        ...dashboard,
        cells: dashboard.cells.map(c => (c.x === x && c.y === y ? newCell : c)),
      }

      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? newDashboard : d)
        ),
      }

      return {...state, ...newState}
    }

    case 'EDIT_CELL_QUERY_STATUS': {
      const {queryID, status} = action.payload

      return {...state, cellQueryStatus: {queryID, status}}
    }

    case TEMPLATE_VARIABLE_SELECTED: {
      const {
        dashboardID,
        templateID,
        values: updatedSelectedValues,
      } = action.payload
      const newDashboards = state.dashboards.map(dashboard => {
        if (dashboard.id === dashboardID) {
          const newTemplates = dashboard.templates.map(staleTemplate => {
            if (staleTemplate.id === templateID) {
              const newValues = staleTemplate.values.map(staleValue => {
                let selected = false
                for (let i = 0; i < updatedSelectedValues.length; i++) {
                  if (updatedSelectedValues[i].value === staleValue.value) {
                    selected = true
                    break
                  }
                }
                return {...staleValue, selected}
              })
              return {...staleTemplate, values: newValues}
            }
            return staleTemplate
          })
          return {...dashboard, templates: newTemplates}
        }
        return dashboard
      })
      return {...state, dashboards: newDashboards}
    }

    case 'EDIT_TEMPLATE_VARIABLE_VALUES': {
      const {dashboardID, templateID, values} = action.payload

      const dashboards = state.dashboards.map(
        dashboard =>
          dashboard.id === dashboardID
            ? {
                ...dashboard,
                templates: dashboard.templates.map(
                  template =>
                    template.id === templateID
                      ? {
                          ...template,
                          values: values.map((value, i) => ({
                            selected: i === 0,
                            value,
                            type: TEMPLATE_VARIABLE_TYPES[template.type],
                          })),
                        }
                      : template
                ),
              }
            : dashboard
      )

      return {...state, dashboards}
    }
  }

  return state
}
