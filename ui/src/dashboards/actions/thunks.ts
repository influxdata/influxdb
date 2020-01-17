// Libraries
import {normalize} from 'normalizr'
import {Dispatch} from 'react'
import {push} from 'react-router-redux'

// APIs
import * as dashAPI from 'src/dashboards/apis'
import * as api from 'src/client'
import * as tempAPI from 'src/templates/api'

// Schemas
import * as schemas from 'src/schemas'

// Actions
import {
  notify,
  PublishNotificationAction,
} from 'src/shared/actions/notifications'
import {
  deleteTimeRange,
  updateTimeRangeFromQueryParams,
} from 'src/dashboards/actions/ranges'
import {setView, setViews} from 'src/dashboards/actions/views'
import {selectValue} from 'src/variables/actions/creators'
import {getVariables, refreshVariableValues} from 'src/variables/actions/thunks'
import {setExportTemplate} from 'src/templates/actions'
import {checkDashboardLimits} from 'src/cloud/actions/limits'
import * as creators from 'src/dashboards/actions/creators'

// Utils
import {addVariableDefaults} from 'src/variables/actions/thunks'
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
import {getAll, getByID} from 'src/resources/selectors'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'

// Types
import {
  Dashboard,
  NewView,
  Cell,
  GetState,
  View,
  DashboardTemplate,
  Label,
  RemoteDataState,
  NewCell,
  DashboardEntities,
  ResourceType,
} from 'src/types'

type Action = creators.Action

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

    const resp = await api.postDashboard({data: newDashboard})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(push(`/orgs/${org.id}/dashboards/${resp.data.id}`))
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

export const cloneDashboard = (dashboard: Dashboard) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const state = getState()

    const org = getOrg(state)
    const dashboards = getAll<Dashboard>(state, ResourceType.Dashboards)
    const allDashboardNames = dashboards.map(d => d.name)
    const clonedName = incrementCloneName(allDashboardNames, dashboard.name)

    const getResp = await api.getDashboard({dashboardID: dashboard.id})

    if (getResp.status !== 200) {
      throw new Error(getResp.data.message)
    }

    const {entities, result} = normalize<Dashboard, DashboardEntities, string>(
      getResp.data,
      schemas.dashboard
    )

    const dash: Dashboard = entities[result]

    const postResp = await api.postDashboard({
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
      api.postDashboardsLabel({
        dashboardID: postResp.data.id,
        data: {labelID: l.id},
      })
    )

    const mappedLabels = await Promise.all(pendingLabels)

    if (mappedLabels.length > 0 && mappedLabels.some(l => l.status !== 201)) {
      throw new Error('An error occurred cloning the labels for this dashboard')
    }

    const clonedViews = await dashAPI.cloneUtilFunc(dash, postResp.data.id)

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

export const getDashboards = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  try {
    const org = getOrg(getState())
    const {setDashboards} = creators

    dispatch(setDashboards(RemoteDataState.Loading))
    const resp = await api.getDashboards({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const dashboards = normalize<Dashboard, DashboardEntities, string[]>(
      resp.data.dashboards,
      schemas.arrayOfDashboards
    )

    dispatch(setDashboards(RemoteDataState.Done, dashboards))
  } catch (error) {
    dispatch(creators.setDashboards(RemoteDataState.Error))
    console.error(error)
    throw error
  }
}

export const createDashboardFromTemplate = (
  template: DashboardTemplate
) => async (dispatch, getState: GetState) => {
  try {
    const org = getOrg(getState())

    await tempAPI.createDashboardFromTemplate(template, org.id)

    const resp = await api.getDashboards({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const dashboards = normalize<Dashboard, DashboardEntities, string[]>(
      resp.data.dashboards,
      schemas.arrayOfDashboards
    )

    dispatch(creators.setDashboards(RemoteDataState.Done, dashboards))
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

export const deleteDashboard = (dashboard: Dashboard) => async (
  dispatch
): Promise<void> => {
  dispatch(creators.removeDashboard(dashboard.id))
  dispatch(deleteTimeRange(dashboard.id))

  try {
    const resp = await api.deleteDashboard({dashboardID: dashboard.id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(notify(copy.dashboardDeleted(dashboard.name)))
    dispatch(checkDashboardLimits())
  } catch (error) {
    dispatch(
      notify(copy.dashboardDeleteFailed(dashboard.name, error.data.message))
    )

    dispatch(creators.deleteDashboardFailed(dashboard))
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

export const getDashboard = (dashboardID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    // Fetch the dashboard and all variables a user has access to
    const [resp] = await Promise.all([
      api.getDashboard({dashboardID}),
      dispatch(getVariables()),
    ])

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      schemas.dashboard
    )
    const {cells, id}: Dashboard = normDash.entities.dashboards[normDash.result]

    // Fetch all the views in use on the dashboard
    const views = await Promise.all(
      cells.map(cell => dashAPI.getView(id, cell.id))
    )

    dispatch(setViews(RemoteDataState.Done, views))

    // Ensure the values for the variables in use on the dashboard are populated
    await dispatch(refreshDashboardVariableValues(id, views))

    // Now that all the necessary state has been loaded, set the dashboard
    dispatch(creators.setDashboard(normDash))
    dispatch(updateTimeRangeFromQueryParams(id))
  } catch (error) {
    const org = getOrg(getState())
    dispatch(push(`/orgs/${org.id}/dashboards`))
    dispatch(notify(copy.dashboardGetFailed(dashboardID, error.message)))
    return
  }
}

export const updateDashboard = (dashboard: Dashboard) => async (
  dispatch: Dispatch<creators.Action | PublishNotificationAction>
): Promise<void> => {
  try {
    const resp = await api.patchDashboard({
      dashboardID: dashboard.id,
      data: dashboard,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedDashboard = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      schemas.dashboard
    )

    dispatch(creators.editDashboard(updatedDashboard))
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
    let dashboard = getByID<Dashboard>(
      state,
      ResourceType.Dashboards,
      dashboardID
    )

    if (!dashboard) {
      const resp = await api.getDashboard({dashboardID})
      if (resp.status !== 200) {
        throw new Error(resp.data.message)
      }

      const {entities, result} = normalize<
        Dashboard,
        DashboardEntities,
        string
      >(resp.data, schemas.dashboard)

      dashboard = entities.dashboards[result]
    }

    const cell: NewCell = getNewDashboardCell(dashboard, clonedCell)

    // Create the cell
    const cellResp = await api.postDashboardsCell({dashboardID, data: cell})

    if (cellResp.status !== 201) {
      throw new Error(cellResp.data.message)
    }

    const createdCell = cellResp.data

    // Create the view and associate it with the cell
    const newView = await dashAPI.updateView(dashboardID, createdCell.id, view)

    // Update the dashboard with the new cell
    let updatedDashboard: Dashboard = {
      ...dashboard,
      cells: [...dashboard.cells, {...cellResp.data, dashboardID}],
    }

    const resp = await api.patchDashboard({dashboardID, data: updatedDashboard})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      schemas.dashboard
    )
    const {entities, result} = normDash

    updatedDashboard = entities[result]

    // Refresh variables in use on dashboard
    const views = [...getViewsForDashboard(state, dashboard.id), newView]

    await dispatch(refreshDashboardVariableValues(dashboard.id, views))

    dispatch(setView(createdCell.id, newView, RemoteDataState.Done))
    dispatch(creators.editDashboard(normDash))
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
    const newView = await dashAPI.updateView(dashboardID, cellID, view)

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

export const updateCells = (dashboard: Dashboard, cells: Cell[]) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const resp = await api.putDashboardsCells({
      dashboardID: dashboard.id,
      data: cells,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedDashboard = {
      ...dashboard,
      cells: resp.data.cells,
    }

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      updatedDashboard,
      schemas.dashboard
    )

    dispatch(creators.setDashboard(normDash))
  } catch (error) {
    console.error(error)
  }
}

export const deleteCell = (dashboard: Dashboard, cell: Cell) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const views = getViewsForDashboard(getState(), dashboard.id).filter(
      view => view.cellID !== cell.id
    )

    await Promise.all([
      api.deleteDashboardsCell({dashboardID: dashboard.id, cellID: cell.id}),
      dispatch(refreshDashboardVariableValues(dashboard.id, views)),
    ])

    dispatch(creators.removeCell(dashboard.id, cell.id))
    dispatch(notify(copy.cellDeleted()))
  } catch (error) {
    console.error(error)
  }
}

export const copyDashboardCell = (dashboard: Dashboard, cell: Cell) => (
  dispatch: Dispatch<Action | PublishNotificationAction>
) => {
  try {
    const clonedCell = getClonedDashboardCell(dashboard, cell)
    const updatedDashboard = {
      ...dashboard,
      cells: [...dashboard.cells, clonedCell],
    }

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      updatedDashboard,
      schemas.dashboard
    )

    dispatch(creators.setDashboard(normDash))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}

export const addDashboardLabel = (dashboardID: string, label: Label) => async (
  dispatch: Dispatch<Action | PublishNotificationAction>
) => {
  try {
    const resp = await api.postDashboardsLabel({
      dashboardID,
      data: {labelID: label.id},
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const lab = addLabelDefaults(resp.data.label)

    dispatch(creators.addDashboardLabel(dashboardID, lab))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addDashboardLabelFailed()))
  }
}

export const removeDashboardLabel = (
  dashboardID: string,
  label: Label
) => async (dispatch: Dispatch<Action | PublishNotificationAction>) => {
  try {
    const resp = await api.deleteDashboardsLabel({
      dashboardID,
      labelID: label.id,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(creators.removeDashboardLabel(dashboardID, label.id))
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
  const state = getState()
  const variables = getHydratedVariables(state, dashboardID)
  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    dashboardID
  )

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

    const dashResp = await api.getDashboard({dashboardID})

    if (dashResp.status !== 200) {
      throw new Error(dashResp.data.message)
    }

    const {entities, result} = normalize<Dashboard, DashboardEntities, string>(
      dashResp.data,
      schemas.dashboard
    )

    const dashboard: Dashboard = entities.dashboards[result]

    const pendingViews = dashboard.cells.map(c =>
      dashAPI.getView(dashboardID, c.id)
    )
    const views = await Promise.all(pendingViews)
    const resp = await api.getVariables({query: {orgID: org.id}})
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
