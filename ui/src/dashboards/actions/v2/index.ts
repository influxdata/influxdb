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
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2'
import {client} from 'src/utils/api'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  deleteTimeRange,
  updateTimeRangeFromQueryParams,
  DeleteTimeRangeAction,
} from 'src/dashboards/actions/v2/ranges'
import {
  importDashboardSucceeded,
  importDashboardFailed,
} from 'src/shared/copy/notifications'
import {setView, SetViewAction, setViews} from 'src/dashboards/actions/v2/views'
import {getVariables, refreshVariableValues} from 'src/variables/actions'

// Utils
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {getVariablesForOrg} from 'src/variables/selectors'
import {getViewsForDashboard} from 'src/dashboards/selectors'
import {
  getNewDashboardCell,
  getClonedDashboardCell,
} from 'src/dashboards/utils/cellGetters'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from 'src/types'
import {PublishNotificationAction} from 'src/types/actions/notifications'
import {CreateCell, IDashboardTemplate} from '@influxdata/influx'
import {Dashboard, NewView, Cell, GetState, View} from 'src/types/v2'
import {ILabel} from '@influxdata/influx'

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
    labels: ILabel[]
  }
}

interface RemoveDashboardLabelsAction {
  type: ActionTypes.RemoveDashboardLabels
  payload: {
    dashboardID: string
    labels: ILabel[]
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
  labels: ILabel[]
): AddDashboardLabelsAction => ({
  type: ActionTypes.AddDashboardLabels,
  payload: {dashboardID, labels},
})

export const removeDashboardLabels = (
  dashboardID: string,
  labels: ILabel[]
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

export const createDashboardFromTemplate = (
  template: IDashboardTemplate,
  orgID: string
) => async dispatch => {
  try {
    await client.dashboards.createFromTemplate(template, orgID)

    dispatch(notify(importDashboardSucceeded()))
  } catch (error) {
    dispatch(notify(importDashboardFailed(error)))
  }
}

export const importDashboardAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    await createDashboardAJAX(dashboard)
    const dashboards = await getDashboardsAJAX()

    dispatch(loadDashboards(dashboards))
    dispatch(notify(copy.dashboardImported()))
  } catch (error) {
    dispatch(notify(copy.dashboardImportFailed('Could not upload dashboard')))
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

export const refreshDashboardVariableValues = (
  dashboard: Dashboard,
  nextViews: View[]
) => (dispatch, getState: GetState) => {
  const variables = getVariablesForOrg(getState(), dashboard.orgID)
  const variablesInUse = filterUnusedVars(variables, nextViews)

  return dispatch(
    refreshVariableValues(dashboard.id, dashboard.orgID, variablesInUse)
  )
}

export const getDashboardAsync = (dashboardID: string) => async (
  dispatch
): Promise<void> => {
  try {
    // Fetch the dashboard and all variables a user has access to
    const [dashboard] = await Promise.all([
      getDashboardAJAX(dashboardID),
      dispatch(getVariables()),
    ])

    // Fetch all the views in use on the dashboard
    const views = await Promise.all(
      dashboard.cells.map(cell => getViewAJAX(dashboard.id, cell.id))
    )

    dispatch(setViews(RemoteDataState.Done, views))

    // Ensure the values for the variables in use on the dashboard are populated
    await dispatch(refreshDashboardVariableValues(dashboard, views))

    // Now that all the necessary state has been loaded, set the dashboard
    dispatch(loadDashboard(dashboard))
  } catch {
    dispatch(replace(`/dashboards`))
    dispatch(notify(copy.dashboardGetFailed(dashboardID)))

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

export const createCellWithView = (
  dashboard: Dashboard,
  view: NewView,
  clonedCell?: Cell
) => async (dispatch, getState: GetState): Promise<void> => {
  try {
    const cell: CreateCell = getNewDashboardCell(dashboard, clonedCell)

    // Create the cell
    const createdCell = await addCellAJAX(dashboard.id, cell)

    // Create the view and associate it with the cell
    const newView = await updateViewAJAX(dashboard.id, createdCell.id, view)

    // Update the dashboard with the new cell
    let updatedDashboard: Dashboard = {
      ...dashboard,
      cells: [...dashboard.cells, createdCell],
    }

    updatedDashboard = await updateDashboardAJAX(dashboard)

    // Refresh variables in use on dashboard
    const views = [...getViewsForDashboard(getState(), dashboard.id), newView]

    await dispatch(refreshDashboardVariableValues(dashboard, views))

    dispatch(setView(createdCell.id, newView, RemoteDataState.Done))
    dispatch(updateDashboard(updatedDashboard))
  } catch {
    notify(copy.cellAddFailed())
  }
}

export const updateView = (dashboard: Dashboard, view: View) => async (
  dispatch,
  getState: GetState
) => {
  const cellID = view.cellID

  try {
    const newView = await updateViewAJAX(dashboard.id, cellID, view)

    const views = getViewsForDashboard(getState(), dashboard.id)

    views.splice(views.findIndex(v => v.id === newView.id), 1, newView)

    await dispatch(refreshDashboardVariableValues(dashboard, views))

    dispatch(setView(cellID, newView, RemoteDataState.Done))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.cellUpdateFailed()))
    dispatch(setView(cellID, null, RemoteDataState.Error))
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
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const views = getViewsForDashboard(getState(), dashboard.id).filter(
      view => view.cellID !== cell.id
    )

    await Promise.all([
      deleteCellAJAX(dashboard.id, cell),
      dispatch(refreshDashboardVariableValues(dashboard, views)),
    ])

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
  labels: ILabel[]
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
  labels: ILabel[]
) => async (dispatch: Dispatch<Action>) => {
  try {
    await removeDashboardLabelsAJAX(dashboardID, labels)
    dispatch(removeDashboardLabels(dashboardID, labels))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removedDashboardLabelFailed()))
  }
}
