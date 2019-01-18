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
  addDashboardLabels as addDashboardLabelsAJAX,
  removeDashboardLabels as removeDashboardLabelsAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2'

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
import {CreateCell, Label} from 'src/api'
import {Dashboard, NewView, Cell} from 'src/types/v2'

export enum ActionTypes {
  LoadDashboards = 'LOAD_DASHBOARDS',
  LoadDashboard = 'LOAD_DASHBOARD',
  DeleteDashboard = 'DELETE_DASHBOARD',
  DeleteDashboardFailed = 'DELETE_DASHBOARD_FAILED',
  UpdateDashboard = 'UPDATE_DASHBOARD',
  DeleteCell = 'DELETE_CELL',
  AddDashboardLabels = 'ADD_DASHBOARD_LABELS',
  RemoveDashboardLabels = 'REMOVE_DASHBOARD_LABELS',
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
  | AddDashboardLabelsAction
  | RemoveDashboardLabelsAction

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

interface AddDashboardLabelsAction {
  type: ActionTypes.AddDashboardLabels
  payload: {
    dashboardID: string
    labels: Label[]
  }
}

interface RemoveDashboardLabelsAction {
  type: ActionTypes.RemoveDashboardLabels
  payload: {
    dashboardID: string
    labels: Label[]
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

export const addDashboardLabels = (
  dashboardID: string,
  labels: Label[]
): AddDashboardLabelsAction => ({
  type: ActionTypes.AddDashboardLabels,
  payload: {dashboardID, labels},
})

export const removeDashboardLabels = (
  dashboardID: string,
  labels: Label[]
): RemoveDashboardLabelsAction => ({
  type: ActionTypes.RemoveDashboardLabels,
  payload: {dashboardID, labels},
})

// Thunks

export const getDashboardsAsync = () => async (
  dispatch: Dispatch<Action>
): Promise<Dashboard[]> => {
  try {
    const dashboards = await getDashboardsAJAX()
    dispatch(loadDashboards(dashboards))
    return dashboards
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const importDashboardAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    await createDashboardAJAX(dashboard)
    const dashboards = await getDashboardsAJAX()

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
    await deleteDashboardAJAX(dashboard)
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
    const createdCell = await addCellAJAX(dashboard.id, cell)
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

export const createCellWithView = (
  dashboard: Dashboard,
  view: NewView,
  clonedCell?: Cell
) => async (dispatch: Dispatch<Action>): Promise<void> => {
  try {
    const cell: CreateCell = getNewDashboardCell(dashboard, clonedCell)
    const createdCell = await addCellAJAX(dashboard.id, cell)
    const updatedView = await updateViewAJAX(dashboard.id, createdCell.id, view)

    let updatedDashboard: Dashboard = {
      ...dashboard,
      cells: [...dashboard.cells, createdCell],
    }

    updatedDashboard = await updateDashboardAJAX(dashboard)

    dispatch(setView(createdCell.id, updatedView, RemoteDataState.Done))
    dispatch(updateDashboard(updatedDashboard))
  } catch {
    notify(copy.cellAddFailed())
  }
}

export const updateCellsAsync = (dashboard: Dashboard, cells: Cell[]) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const updatedCells = await updateCellsAJAX(dashboard.id, cells)
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
    await deleteCellAJAX(dashboard.id, cell)
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
    const updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, clonedCell],
    }

    dispatch(loadDashboard(updatedDashboard))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}

export const addDashboardLabelsAsync = (
  dashboardID: string,
  labels: Label[]
) => async (dispatch: Dispatch<Action>) => {
  try {
    const newLabels = await addDashboardLabelsAJAX(dashboardID, labels)

    dispatch(addDashboardLabels(dashboardID, newLabels))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addDashboardLabelFailed()))
  }
}

export const removeDashboardLabelsAsync = (
  dashboardID: string,
  labels: Label[]
) => async (dispatch: Dispatch<Action>) => {
  try {
    await removeDashboardLabelsAJAX(dashboardID, labels)
    dispatch(removeDashboardLabels(dashboardID, labels))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removedDashboardLabelFailed()))
  }
}
