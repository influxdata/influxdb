import {Dashboard, Cell, Label, RemoteDataState} from 'src/types'

export const SET_DASHBOARDS = 'SET_DASHBOARDS'
export const SET_DASHBOARD = 'SET_DASHBOARD'
export const REMOVE_DASHBOARD = 'REMOVE_DASHBOARD'
export const DELETE_DASHBOARD_FAILED = 'DELETE_DASHBOARD_FAILED'
export const EDIT_DASHBOARD = 'EDIT_DASHBOARD'
export const REMOVE_CELL = 'REMOVE_CELL'
export const ADD_DASHBOARD_LABEL = 'ADD_DASHBOARD_LABEL'
export const REMOVE_DASHBOARD_LABEL = 'REMOVE_DASHBOARD_LABEL'

export type Action =
  | ReturnType<typeof setDashboards>
  | ReturnType<typeof removeDashboard>
  | ReturnType<typeof setDashboard>
  | ReturnType<typeof editDashboard>
  | ReturnType<typeof removeCell>
  | ReturnType<typeof deleteDashboardFailed>
  | ReturnType<typeof addDashboardLabel>
  | ReturnType<typeof removeDashboardLabel>

// Action Creators
export const editDashboard = (dashboard: Dashboard) =>
  ({
    type: EDIT_DASHBOARD,
    payload: {dashboard},
  } as const)

export const setDashboards = (status: RemoteDataState, list?: Dashboard[]) => {
  if (list) {
    list = list.map(obj => {
      if (obj.name === undefined) {
        obj.name = ''
      }
      if (obj.meta === undefined) {
        obj.meta = {}
      }
      if (obj.meta.updatedAt === undefined) {
        obj.meta.updatedAt = new Date().toDateString()
      }
      return obj
    })
  }

  return {
    type: SET_DASHBOARDS,
    payload: {
      status,
      list,
    },
  } as const
}

export const setDashboard = (dashboard: Dashboard) =>
  ({
    type: SET_DASHBOARD,
    payload: {dashboard},
  } as const)

export const removeDashboard = (id: string) =>
  ({
    type: REMOVE_DASHBOARD,
    payload: {id},
  } as const)

export const deleteDashboardFailed = (dashboard: Dashboard) =>
  ({
    type: DELETE_DASHBOARD_FAILED,
    payload: {dashboard},
  } as const)

export const removeCell = (dashboard: Dashboard, cell: Cell) =>
  ({
    type: REMOVE_CELL,
    payload: {dashboard, cell},
  } as const)

export const addDashboardLabel = (dashboardID: string, label: Label) =>
  ({
    type: ADD_DASHBOARD_LABEL,
    payload: {dashboardID, label},
  } as const)

export const removeDashboardLabel = (dashboardID: string, label: Label) =>
  ({
    type: REMOVE_DASHBOARD_LABEL,
    payload: {dashboardID, label},
  } as const)
