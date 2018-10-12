// Libraries
import {Dispatch} from 'redux'
import {replace} from 'react-router-redux'

// APIs
import {
  getDashboard as getDashboardAJAX,
  getDashboards as getDashboardsAJAX,
  createDashboard as createDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboard as updateDashboardAJAX,
  updateCells as updateCellsAJAX,
  addCell as addCellAJAX,
  deleteCell as deleteCellAJAX,
  copyCell as copyCellAJAX,
} from 'src/dashboards/apis/v2'
import {createView as createViewAJAX} from 'src/dashboards/apis/v2/view'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  deleteTimeRange,
  updateTimeRangeFromQueryParams,
  DeleteTimeRangeAction,
} from 'src/dashboards/actions/v2/ranges'
import {setView, SetViewAction} from 'src/dashboards/actions/v2/views'

// Utils
import {
  getNewDashboardCell,
  getClonedDashboardCell,
} from 'src/dashboards/utils/cellGetters'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from 'src/types'
import {PublishNotificationAction} from 'src/types/actions/notifications'
import {Dashboard, Cell, View, AppState} from 'src/types/v2'

export enum ActionTypes {
  LoadDashboards = 'LOAD_DASHBOARDS',
  LoadDashboard = 'LOAD_DASHBOARD',
  DeleteDashboard = 'DELETE_DASHBOARD',
  DeleteDashboardFailed = 'DELETE_DASHBOARD_FAILED',
  UpdateDashboard = 'UPDATE_DASHBOARD',
  DeleteCell = 'DELETE_CELL',
}

export type Action =
  | LoadDashboardsAction
  | DeleteDashboardAction
  | LoadDashboardAction
  | UpdateDashboardAction
  | DeleteCellAction
  | PublishNotificationAction
  | SetViewAction
  | DeleteTimeRangeAction
  | DeleteDashboardFailedAction

interface DeleteCellAction {
  type: ActionTypes.DeleteCell
  payload: {
    dashboard: Dashboard
    cell: Cell
  }
}

interface UpdateDashboardAction {
  type: ActionTypes.UpdateDashboard
  payload: {
    dashboard: Dashboard
  }
}

interface LoadDashboardsAction {
  type: ActionTypes.LoadDashboards
  payload: {
    dashboards: Dashboard[]
  }
}

interface DeleteDashboardAction {
  type: ActionTypes.DeleteDashboard
  payload: {
    dashboardID: string
  }
}

interface DeleteDashboardFailedAction {
  type: ActionTypes.DeleteDashboardFailed
  payload: {
    dashboard: Dashboard
  }
}

interface LoadDashboardAction {
  type: ActionTypes.LoadDashboard
  payload: {
    dashboard: Dashboard
  }
}

// Action Creators

export const updateDashboard = (
  dashboard: Dashboard
): UpdateDashboardAction => ({
  type: ActionTypes.UpdateDashboard,
  payload: {dashboard},
})

export const loadDashboards = (
  dashboards: Dashboard[]
): LoadDashboardsAction => ({
  type: ActionTypes.LoadDashboards,
  payload: {
    dashboards,
  },
})

export const loadDashboard = (dashboard: Dashboard): LoadDashboardAction => ({
  type: ActionTypes.LoadDashboard,
  payload: {dashboard},
})

export const deleteDashboard = (
  dashboardID: string
): DeleteDashboardAction => ({
  type: ActionTypes.DeleteDashboard,
  payload: {dashboardID},
})

export const deleteDashboardFailed = (
  dashboard: Dashboard
): DeleteDashboardFailedAction => ({
  type: ActionTypes.DeleteDashboardFailed,
  payload: {dashboard},
})

export const deleteCell = (
  dashboard: Dashboard,
  cell: Cell
): DeleteCellAction => ({
  type: ActionTypes.DeleteCell,
  payload: {dashboard, cell},
})

// Thunks

export const getDashboardsAsync = (url: string) => async (
  dispatch: Dispatch<Action>
): Promise<Dashboard[]> => {
  try {
    const dashboards = await getDashboardsAJAX(url)
    dispatch(loadDashboards(dashboards))
    return dashboards
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const importDashboardAsync = (
  url: string,
  dashboard: Dashboard
) => async (dispatch: Dispatch<Action>): Promise<void> => {
  try {
    await createDashboardAJAX(url, dashboard)
    const dashboards = await getDashboardsAJAX(url)

    dispatch(loadDashboards(dashboards))
    dispatch(notify(copy.dashboardImported(name)))
  } catch (error) {
    dispatch(
      notify(copy.dashboardImportFailed('', 'Could not upload dashboard'))
    )
    console.error(error)
  }
}

export const deleteDashboardAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(deleteDashboard(dashboard.id))
  dispatch(deleteTimeRange(dashboard.id))

  try {
    await deleteDashboardAJAX(dashboard.links.self)
    dispatch(notify(copy.dashboardDeleted(dashboard.name)))
  } catch (error) {
    dispatch(
      notify(copy.dashboardDeleteFailed(dashboard.name, error.data.message))
    )

    dispatch(deleteDashboardFailed(dashboard))
  }
}

export const getDashboardAsync = (dashboardID: string) => async (
  dispatch
): Promise<void> => {
  try {
    const dashboard = await getDashboardAJAX(dashboardID)
    dispatch(loadDashboard(dashboard))
  } catch {
    dispatch(replace(`/dashboards`))
    dispatch(notify(copy.dashboardNotFound(dashboardID)))

    return
  }

  dispatch(updateTimeRangeFromQueryParams(dashboardID))
}

export const updateDashboardAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const updatedDashboard = await updateDashboardAJAX(dashboard)
    dispatch(updateDashboard(updatedDashboard))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.dashboardUpdateFailed()))
  }
}

export const addCellAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  const cell = getNewDashboardCell(dashboard)

  try {
    const createdCell = await addCellAJAX(dashboard.links.cells, cell)
    const updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, createdCell],
    }

    dispatch(loadDashboard(updatedDashboard))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}

export const createCellWithView = (dashboard: Dashboard, view: View) => async (
  dispatch: Dispatch<Action>,
  getState: () => AppState
): Promise<void> => {
  try {
    const viewsLink = getState().links.views
    const createdView = await createViewAJAX(viewsLink, view)

    const cell = {
      ...getNewDashboardCell(dashboard),
      viewID: createdView.id,
    }

    const createdCell = await addCellAJAX(dashboard.links.cells, cell)

    let updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, createdCell],
    }

    updatedDashboard = await updateDashboardAJAX(dashboard)

    dispatch(setView(createdView.id, createdView, RemoteDataState.Done))
    dispatch(updateDashboard(updatedDashboard))
  } catch {
    notify(copy.cellAddFailed())
  }
}

export const updateCellsAsync = (dashboard: Dashboard, cells: Cell[]) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const updatedCells = await updateCellsAJAX(dashboard.links.cells, cells)
    const updatedDashboard = {
      ...dashboard,
      cells: updatedCells,
    }

    dispatch(loadDashboard(updatedDashboard))
  } catch (error) {
    console.error(error)
  }
}

export const deleteCellAsync = (dashboard: Dashboard, cell: Cell) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    await deleteCellAJAX(cell.links.self)
    dispatch(deleteCell(dashboard, cell))
    dispatch(notify(copy.cellDeleted()))
  } catch (error) {
    console.error(error)
  }
}

export const copyDashboardCellAsync = (
  dashboard: Dashboard,
  cell: Cell
) => async (dispatch: Dispatch<Action>): Promise<void> => {
  try {
    const clonedCell = getClonedDashboardCell(dashboard, cell)
    const cellFromServer = await copyCellAJAX(cell.links.copy, clonedCell)
    const updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, cellFromServer],
    }

    dispatch(loadDashboard(updatedDashboard))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}
