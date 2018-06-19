import _ from 'lodash'
import {timeRanges} from 'shared/data/timeRanges'
import {NULL_HOVER_TIME} from 'src/shared/constants/tableGraph'

import {applyDashboardTempVarOverrides} from 'src/dashboards/utils/tempVars'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 1h')

export const initialState = {
  dashboards: [],
  timeRange: {lower, upper},
  zoomedTimeRange: {lower: null, upper: null},
  isEditMode: false,
  cellQueryStatus: {queryID: null, status: null},
  hoverTime: NULL_HOVER_TIME,
  activeCellID: '',
}

import {TEMPLATE_VARIABLE_TYPES} from 'src/tempVars/constants'

const ui = (state = initialState, action) => {
  switch (action.type) {
    case 'LOAD_DASHBOARDS': {
      const {dashboards} = action.payload
      const newState = {
        dashboards,
      }

      return {...state, ...newState}
    }

    case 'LOAD_DASHBOARD': {
      const {dashboard} = action.payload
      const newDashboards = _.unionBy([dashboard], state.dashboards, 'id')

      return {...state, dashboards: newDashboards}
    }

    case 'SET_DASHBOARD_TIME_RANGE': {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    case 'SET_DASHBOARD_ZOOMED_TIME_RANGE': {
      const {zoomedTimeRange} = action.payload

      return {...state, zoomedTimeRange}
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

    case 'CREATE_DASHBOARD': {
      const {dashboard} = action.payload
      const newState = {
        dashboards: [...state.dashboards, dashboard],
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

    case 'EDIT_CELL_QUERY_STATUS': {
      const {queryID, status} = action.payload

      return {...state, cellQueryStatus: {queryID, status}}
    }

    case 'TEMPLATE_VARIABLE_SELECTED': {
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

    case 'TEMPLATE_VARIABLES_SELECTED_BY_NAME': {
      const {dashboardID, queryParams} = action.payload

      const newDashboards = state.dashboards.map(
        oldDashboard =>
          oldDashboard.id === dashboardID
            ? applyDashboardTempVarOverrides(oldDashboard, queryParams)
            : oldDashboard
      )

      return {...state, dashboards: newDashboards}
    }

    case 'EDIT_TEMPLATE_VARIABLE_VALUES': {
      const {dashboardID, templateID, values} = action.payload

      const dashboards = state.dashboards.map(dashboard => {
        if (dashboard.id !== dashboardID) {
          return dashboard
        }

        const templates = dashboard.templates.map(template => {
          if (template.id !== templateID || template.type === 'csv') {
            return template
          }

          const selectedValue = _.get(template, 'values', []).find(
            v => v.selected
          )

          const v = values.map(value => ({
            selected: _.get(selectedValue, 'value') === value,
            value,
            type: TEMPLATE_VARIABLE_TYPES[template.type],
          }))

          return {
            ...template,
            values: v,
          }
        })

        return {
          ...dashboard,
          templates,
        }
      })

      return {...state, dashboards}
    }

    case 'SET_HOVER_TIME': {
      const {hoverTime} = action.payload

      return {...state, hoverTime}
    }

    case 'SET_ACTIVE_CELL': {
      const {activeCellID} = action.payload
      return {...state, activeCellID}
    }
  }

  return state
}

export default ui
