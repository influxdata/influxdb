import {Dashboard, Cell, Label, RemoteDataState} from 'src/types'

export enum ActionTypes {
  SetDashboards = 'SET_DASHBOARDS',
  SetDashboard = 'SET_DASHBOARD',
  RemoveDashboard = 'REMOVE_DASHBOARD',
  DeleteDashboardFailed = 'DELETE_DASHBOARD_FAILED',
  EditDashboard = 'EDIT_DASHBOARD',
  RemoveCell = 'REMOVE_CELL',
  AddDashboardLabel = 'ADD_DASHBOARD_LABEL',
  RemoveDashboardLabel = 'REMOVE_DASHBOARD_LABEL',
}

export type Action =
  | SetDashboardsAction
  | RemoveDashboardAction
  | SetDashboardAction
  | EditDashboardAction
  | RemoveCellAction
  | DeleteDashboardFailedAction
  | AddDashboardLabelAction
  | RemoveDashboardLabelAction

interface RemoveCellAction {
  type: ActionTypes.RemoveCell
  payload: {
    dashboard: Dashboard
    cell: Cell
  }
}

interface EditDashboardAction {
  type: ActionTypes.EditDashboard
  payload: {
    dashboard: Dashboard
  }
}

interface SetDashboardsAction {
  type: ActionTypes.SetDashboards
  payload: {
    status: RemoteDataState
    list: Dashboard[]
  }
}

interface RemoveDashboardAction {
  type: ActionTypes.RemoveDashboard
  payload: {
    id: string
  }
}

interface DeleteDashboardFailedAction {
  type: ActionTypes.DeleteDashboardFailed
  payload: {
    dashboard: Dashboard
  }
}

interface SetDashboardAction {
  type: ActionTypes.SetDashboard
  payload: {
    dashboard: Dashboard
  }
}

interface AddDashboardLabelAction {
  type: ActionTypes.AddDashboardLabel
  payload: {
    dashboardID: string
    label: Label
  }
}

interface RemoveDashboardLabelAction {
  type: ActionTypes.RemoveDashboardLabel
  payload: {
    dashboardID: string
    label: Label
  }
}

// Action Creators

export const editDashboard = (dashboard: Dashboard): EditDashboardAction => ({
  type: ActionTypes.EditDashboard,
  payload: {dashboard},
})

export const setDashboards = (
  status: RemoteDataState,
  list?: Dashboard[]
): SetDashboardsAction => {
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
    type: ActionTypes.SetDashboards,
    payload: {
      status,
      list,
    },
  }
}

export const setDashboard = (dashboard: Dashboard): SetDashboardAction => ({
  type: ActionTypes.SetDashboard,
  payload: {dashboard},
})

export const removeDashboard = (id: string): RemoveDashboardAction => ({
  type: ActionTypes.RemoveDashboard,
  payload: {id},
})

export const deleteDashboardFailed = (
  dashboard: Dashboard
): DeleteDashboardFailedAction => ({
  type: ActionTypes.DeleteDashboardFailed,
  payload: {dashboard},
})

export const removeCell = (
  dashboard: Dashboard,
  cell: Cell
): RemoveCellAction => ({
  type: ActionTypes.RemoveCell,
  payload: {dashboard, cell},
})

export const addDashboardLabel = (
  dashboardID: string,
  label: Label
): AddDashboardLabelAction => ({
  type: ActionTypes.AddDashboardLabel,
  payload: {dashboardID, label},
})

export const removeDashboardLabel = (
  dashboardID: string,
  label: Label
): RemoveDashboardLabelAction => ({
  type: ActionTypes.RemoveDashboardLabel,
  payload: {dashboardID, label},
})
