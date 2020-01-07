// Libraries
import {Dispatch} from 'redux'
import {push} from 'react-router-redux'

// APIs
import {
  createDashboard as createDashboardAJAX,
  getDashboard as getDashboardAJAX,
  getDashboards as getDashboardsAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboard as updateDashboardAJAX,
  updateCells as updateCellsAJAX,
  addCell as addCellAJAX,
  deleteCell as deleteCellAJAX,
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis'
import {
  getVariables as apiGetVariables,
  getDashboard as apiGetDashboard,
  postDashboard as apiPostDashboard,
  postDashboardsCell as apiPostDashboardsCell,
  postDashboardsLabel as apiPostDashboardsLabel,
  deleteDashboardsLabel as apiDeleteDashboardsLabel,
  patchDashboardsCellsView as apiPatchDashboardsCellsView,
  getDashboardsCellsView as apiGetDashboardsCellsView,
} from 'src/client'
import {createDashboardFromTemplate as createDashboardFromTemplateAJAX} from 'src/templates/api'

// Actions
import {
  notify,
  PublishNotificationAction,
} from 'src/shared/actions/notifications'
import {
  deleteTimeRange,
  updateTimeRangeFromQueryParams,
  DeleteTimeRangeAction,
} from 'src/dashboards/actions/ranges'
import {setView, SetViewAction, setViews} from 'src/dashboards/actions/views'
import {
  getVariables,
  refreshVariableValues,
  selectValue,
} from 'src/variables/actions'
import {setExportTemplate} from 'src/templates/actions'
import {checkDashboardLimits} from 'src/cloud/actions/limits'

// Utils
import {addVariableDefaults} from 'src/variables/actions'
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {
  extractVariablesList,
  getHydratedVariables,
} from 'src/variables/selectors'
import {getViewsForDashboard} from 'src/dashboards/selectors'
import {
  getNewDashboardCell,
  getClonedDashboardCell,
} from 'src/dashboards/utils/cellGetters'
import {dashboardToTemplate} from 'src/shared/utils/resourceToTemplate'
import {exportVariables} from 'src/variables/utils/exportVariables'
import {getSaveableView} from 'src/timeMachine/selectors'
import {incrementCloneName} from 'src/utils/naming'
import {isLimitError} from 'src/cloud/utils/limits'
import {getOrg} from 'src/organizations/selectors'
import {addLabelDefaults} from 'src/labels/utils'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'

// Types
import {CreateCell} from '@influxdata/influx'
import {
  Dashboard,
  NewView,
  Cell,
  GetState,
  View,
  DashboardTemplate,
  Label,
  RemoteDataState,
} from 'src/types'
import {
  Dashboard as IDashboard,
  Cell as ICell,
  DashboardWithViewProperties,
} from 'src/client'

export const addDashboardIDToCells = (
  cells: ICell[],
  dashboardID: string
): Cell[] => {
  return cells.map(c => {
    return {...c, dashboardID}
  })
}

export const addDashboardDefaults = (
  dashboard: IDashboard | DashboardWithViewProperties
): Dashboard => {
  return {
    ...dashboard,
    cells: addDashboardIDToCells(dashboard.cells, dashboard.id) || [],
    id: dashboard.id || '',
    labels: (dashboard.labels || []).map(addLabelDefaults),
    name: dashboard.name || '',
    orgID: dashboard.orgID || '',
  }
}

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
  | SetViewAction
  | DeleteTimeRangeAction
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

// Thunks

export const createDashboard = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const org = getOrg(getState())

    const newDashboard = {
      name: DEFAULT_DASHBOARD_NAME,
      cells: [],
      orgID: org.id,
    }

    const data = await createDashboardAJAX(newDashboard)
    dispatch(push(`/orgs/${org.id}/dashboards/${data.id}`))
    dispatch(checkDashboardLimits())
  } catch (error) {
    console.error(error)

    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('dashboards')))
    } else {
      dispatch(notify(copy.dashboardCreateFailed()))
    }
  }
}

export const cloneUtilFunc = async (dash: Dashboard, id: string) => {
  const cells = dash.cells
  const pendingViews = cells.map(cell =>
    apiGetDashboardsCellsView({
      dashboardID: dash.id,
      cellID: cell.id,
    }).then(res => {
      return {
        ...res,
        cellID: cell.id,
      }
    })
  )
  const views = await Promise.all(pendingViews)

  if (views.length > 0 && views.some(v => v.status !== 200)) {
    throw new Error('An error occurred cloning the dashboard')
  }

  return views.map(async v => {
    const view = v.data as View
    const cell = cells.find(c => c.id === view.id)

    if (cell && id) {
      const newCell = await apiPostDashboardsCell({
        dashboardID: id,
        data: cell,
      })
      if (newCell.status !== 201) {
        throw new Error('An error occurred cloning the dashboard')
      }
      return apiPatchDashboardsCellsView({
        dashboardID: id,
        cellID: newCell.data.id,
        data: view,
      })
    }
  })
}

export const cloneDashboard = (dashboard: Dashboard) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const {dashboards} = getState()

    const org = getOrg(getState())
    const allDashboardNames = dashboards.list.map(d => d.name)

    const clonedName = incrementCloneName(allDashboardNames, dashboard.name)

    const getResp = await apiGetDashboard({dashboardID: dashboard.id})

    if (getResp.status !== 200) {
      throw new Error(getResp.data.message)
    }

    const dash = addDashboardDefaults(getResp.data)

    const postResp = await apiPostDashboard({
      data: {
        orgID: org.id,
        name: clonedName,
        description: dash.description || '',
      },
    })

    if (postResp.status !== 201) {
      throw new Error(postResp.data.message)
    }

    const pendingLabels = dash.labels.map(l =>
      apiPostDashboardsLabel({
        dashboardID: postResp.data.id,
        data: {labelID: l.id},
      })
    )

    const mappedLabels = await Promise.all(pendingLabels)

    if (mappedLabels.length > 0 && mappedLabels.some(l => l.status !== 201)) {
      throw new Error('An error occurred cloning the labels for this dashboard')
    }

    const clonedViews = await cloneUtilFunc(dash, postResp.data.id)

    const newViews = await Promise.all(clonedViews)

    if (newViews.length > 0 && newViews.some(v => v.status !== 200)) {
      throw new Error('An error occurred cloning the dashboard')
    }

    dispatch(checkDashboardLimits())
    dispatch(push(`/orgs/${org.id}/dashboards/${postResp.data.id}`))
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('dashboards')))
    } else {
      dispatch(notify(copy.dashboardCreateFailed()))
    }
  }
}

export const getDashboardsAsync = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<Dashboard[]> => {
  try {
    const org = getOrg(getState())

    dispatch(setDashboards(RemoteDataState.Loading))
    const dashboards = await getDashboardsAJAX(org.id)
    dispatch(setDashboards(RemoteDataState.Done, dashboards))

    return dashboards
  } catch (error) {
    dispatch(setDashboards(RemoteDataState.Error))
    console.error(error)
    throw error
  }
}

export const createDashboardFromTemplate = (
  template: DashboardTemplate
) => async (dispatch, getState: GetState) => {
  try {
    const org = getOrg(getState())

    await createDashboardFromTemplateAJAX(template, org.id)

    const dashboards = await getDashboardsAJAX(org.id)

    dispatch(setDashboards(RemoteDataState.Done, dashboards))
    dispatch(notify(copy.importDashboardSucceeded()))
    dispatch(checkDashboardLimits())
  } catch (error) {
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('dashboards')))
    } else {
      dispatch(notify(copy.importDashboardFailed(error)))
    }
  }
}

export const deleteDashboardAsync = (dashboard: Dashboard) => async (
  dispatch
): Promise<void> => {
  dispatch(removeDashboard(dashboard.id))
  dispatch(deleteTimeRange(dashboard.id))

  try {
    await deleteDashboardAJAX(dashboard)
    dispatch(notify(copy.dashboardDeleted(dashboard.name)))
    dispatch(checkDashboardLimits())
  } catch (error) {
    dispatch(
      notify(copy.dashboardDeleteFailed(dashboard.name, error.data.message))
    )

    dispatch(deleteDashboardFailed(dashboard))
  }
}

export const refreshDashboardVariableValues = (
  dashboardID: string,
  nextViews: View[]
) => (dispatch, getState: GetState) => {
  const variables = extractVariablesList(getState())
  const variablesInUse = filterUnusedVars(variables, nextViews)

  return dispatch(refreshVariableValues(dashboardID, variablesInUse))
}

export const getDashboardAsync = (dashboardID: string) => async (
  dispatch,
  getState: GetState
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
    await dispatch(refreshDashboardVariableValues(dashboard.id, views))

    // Now that all the necessary state has been loaded, set the dashboard
    dispatch(setDashboard(dashboard))
    dispatch(updateTimeRangeFromQueryParams(dashboardID))
  } catch (error) {
    const org = getOrg(getState())
    dispatch(push(`/orgs/${org.id}/dashboards`))
    dispatch(notify(copy.dashboardGetFailed(dashboardID, error.message)))
    return
  }
}

export const updateDashboardAsync = (dashboard: Dashboard) => async (
  dispatch: Dispatch<Action | PublishNotificationAction>
): Promise<void> => {
  try {
    const updatedDashboard = await updateDashboardAJAX(dashboard)
    dispatch(editDashboard(updatedDashboard))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.dashboardUpdateFailed()))
  }
}

export const createCellWithView = (
  dashboardID: string,
  view: NewView,
  clonedCell?: Cell
) => async (dispatch, getState: GetState): Promise<void> => {
  try {
    const state = getState()
    let dashboard = state.dashboards.list.find(d => d.id === dashboardID)
    if (!dashboard) {
      dashboard = await getDashboardAJAX(dashboardID)
    }

    const cell: CreateCell = getNewDashboardCell(dashboard, clonedCell)

    // Create the cell
    const createdCell = await addCellAJAX(dashboardID, cell)

    // Create the view and associate it with the cell
    const newView = await updateViewAJAX(dashboardID, createdCell.id, view)

    // Update the dashboard with the new cell
    let updatedDashboard: Dashboard = {
      ...dashboard,
      cells: [...dashboard.cells, createdCell],
    }

    updatedDashboard = await updateDashboardAJAX(dashboard)

    // Refresh variables in use on dashboard
    const views = [...getViewsForDashboard(state, dashboard.id), newView]

    await dispatch(refreshDashboardVariableValues(dashboard.id, views))

    dispatch(setView(createdCell.id, newView, RemoteDataState.Done))
    dispatch(editDashboard(updatedDashboard))
  } catch {
    notify(copy.cellAddFailed())
  }
}

export const updateView = (dashboardID: string, view: View) => async (
  dispatch,
  getState: GetState
) => {
  const cellID = view.cellID

  try {
    const newView = await updateViewAJAX(dashboardID, cellID, view)

    const views = getViewsForDashboard(getState(), dashboardID)

    views.splice(views.findIndex(v => v.id === newView.id), 1, newView)

    await dispatch(refreshDashboardVariableValues(dashboardID, views))

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

    dispatch(setDashboard(updatedDashboard))
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
      dispatch(refreshDashboardVariableValues(dashboard.id, views)),
    ])

    dispatch(removeCell(dashboard, cell))
    dispatch(notify(copy.cellDeleted()))
  } catch (error) {
    console.error(error)
  }
}

export const copyDashboardCellAsync = (dashboard: Dashboard, cell: Cell) => (
  dispatch: Dispatch<Action | PublishNotificationAction>
) => {
  try {
    const clonedCell = getClonedDashboardCell(dashboard, cell)
    const updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, clonedCell],
    }

    dispatch(setDashboard(updatedDashboard))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}

export const addDashboardLabelAsync = (
  dashboardID: string,
  label: Label
) => async (dispatch: Dispatch<Action | PublishNotificationAction>) => {
  try {
    const resp = await apiPostDashboardsLabel({
      dashboardID,
      data: {labelID: label.id},
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const lab = addLabelDefaults(resp.data.label)

    dispatch(addDashboardLabel(dashboardID, lab))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addDashboardLabelFailed()))
  }
}

export const removeDashboardLabelAsync = (
  dashboardID: string,
  label: Label
) => async (dispatch: Dispatch<Action | PublishNotificationAction>) => {
  try {
    const resp = await apiDeleteDashboardsLabel({
      dashboardID,
      labelID: label.id,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeDashboardLabel(dashboardID, label))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removedDashboardLabelFailed()))
  }
}

export const selectVariableValue = (
  dashboardID: string,
  variableID: string,
  value: string
) => async (dispatch, getState: GetState): Promise<void> => {
  const variables = getHydratedVariables(getState(), dashboardID)
  const dashboard = getState().dashboards.list.find(d => d.id === dashboardID)

  dispatch(selectValue(dashboardID, variableID, value))

  await dispatch(refreshVariableValues(dashboard.id, variables))
}

export const convertToTemplate = (dashboardID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const org = getOrg(getState())
    const dashboard = await getDashboardAJAX(dashboardID)
    const pendingViews = dashboard.cells.map(c =>
      getViewAJAX(dashboardID, c.id)
    )
    const views = await Promise.all(pendingViews)
    const resp = await apiGetVariables({query: {orgID: org.id}})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }
    const vars = resp.data.variables.map(v => addVariableDefaults(v))
    const variables = filterUnusedVars(vars, views)
    const exportedVariables = exportVariables(variables, vars)
    const dashboardTemplate = dashboardToTemplate(
      dashboard,
      views,
      exportedVariables
    )

    dispatch(setExportTemplate(RemoteDataState.Done, dashboardTemplate))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const saveVEOView = (dashboardID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const view = getSaveableView(getState())

  try {
    if (view.id) {
      await dispatch(updateView(dashboardID, view))
    } else {
      await dispatch(createCellWithView(dashboardID, view))
    }
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.cellAddFailed()))
    throw error
  }
}
