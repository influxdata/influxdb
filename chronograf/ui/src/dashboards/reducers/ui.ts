import _ from 'lodash'
import {timeRanges} from 'src/shared/data/timeRanges'
import {NULL_HOVER_TIME} from 'src/shared/constants/tableGraph'
import {DashboardUIState} from 'src/types/dashboards'
import {Action, ActionType} from 'src/dashboards/actions'

import {TemplateType} from 'src/types/tempVars'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 1h')

export const initialState: DashboardUIState = {
  dashboards: [],
  timeRange: {lower, upper},
  zoomedTimeRange: {lower: null, upper: null},
  isEditMode: false,
  cellQueryStatus: {queryID: null, status: null},
  hoverTime: NULL_HOVER_TIME,
  activeCellID: '',
}

export default (
  state: DashboardUIState = initialState,
  action: Action
): DashboardUIState => {
  switch (action.type) {
    case ActionType.LoadDashboards: {
      const {dashboards} = action.payload
      const newState = {
        dashboards,
      }

      return {...state, ...newState}
    }

    case ActionType.LoadDashboard: {
      const {dashboard} = action.payload
      const newDashboards = _.unionBy([dashboard], state.dashboards, 'id')

      return {...state, dashboards: newDashboards}
    }

    case ActionType.SetDashboardTimeRange: {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    case ActionType.SetDashboardZoomedTimeRange: {
      const {zoomedTimeRange} = action.payload

      return {...state, zoomedTimeRange}
    }

    case ActionType.UpdateDashboard: {
      const {dashboard} = action.payload
      const newState = {
        dashboards: state.dashboards.map(
          d => (d.id === dashboard.id ? dashboard : d)
        ),
      }
      return {...state, ...newState}
    }

    case ActionType.CreateDashboard: {
      const {dashboard} = action.payload
      const newState = {
        dashboards: [...state.dashboards, dashboard],
      }
      return {...state, ...newState}
    }

    case ActionType.DeleteDashboard: {
      const {dashboard} = action.payload
      const newState = {
        dashboards: state.dashboards.filter(d => d.id !== dashboard.id),
      }

      return {...state, ...newState}
    }

    case ActionType.DeleteDashboardFailed: {
      const {dashboard} = action.payload
      const newState = {
        dashboards: [_.cloneDeep(dashboard), ...state.dashboards],
      }
      return {...state, ...newState}
    }

    case ActionType.AddDashboardCell: {
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

    case ActionType.DeleteDashboardCell: {
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

    case ActionType.SyncDashboardCell: {
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

    case ActionType.EditCellQueryStatus: {
      const {queryID, status} = action.payload

      return {...state, cellQueryStatus: {queryID, status}}
    }

    case ActionType.TemplateVariableLocalSelected: {
      const {dashboardID, templateID, value: newValue} = action.payload

      const dashboards = state.dashboards.map(dashboard => {
        if (dashboard.id !== dashboardID) {
          return dashboard
        }

        const templates = dashboard.templates.map(template => {
          if (template.id !== templateID) {
            return template
          }

          let values
          if (template.type === TemplateType.Text) {
            values = [newValue]
          } else {
            values = template.values.map(value => {
              const localSelected = value.value === newValue.value

              return {...value, localSelected}
            })
          }

          return {...template, values}
        })

        return {...dashboard, templates}
      })

      return {...state, dashboards}
    }

    case ActionType.UpdateTemplates: {
      const {templates: updatedTemplates} = action.payload

      const dashboards = state.dashboards.map(dashboard => {
        const templates = dashboard.templates.reduce(
          (acc, existingTemplate) => {
            const updatedTemplate = updatedTemplates.find(
              t => t.id === existingTemplate.id
            )

            if (updatedTemplate) {
              return [...acc, updatedTemplate]
            }

            return [...acc, existingTemplate]
          },
          []
        )

        return {...dashboard, templates}
      })

      return {...state, dashboards}
    }

    case ActionType.SetHoverTime: {
      const {hoverTime} = action.payload

      return {...state, hoverTime}
    }

    case ActionType.SetActiveCell: {
      const {activeCellID} = action.payload
      return {...state, activeCellID}
    }
  }

  return state
}
