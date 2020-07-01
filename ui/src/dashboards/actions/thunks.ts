// Libraries
import {normalize} from 'normalizr'
import {Dispatch} from 'react'
import {push} from 'connected-react-router'

// APIs
import * as dashAPI from 'src/dashboards/apis'
import * as api from 'src/client'
import * as tempAPI from 'src/templates/api'
import {createCellWithView} from 'src/cells/actions/thunks'

// Schemas
import {
  dashboardSchema,
  arrayOfDashboards,
  labelSchema,
  arrayOfViews,
  arrayOfCells,
} from 'src/schemas'
import {viewsFromCells} from 'src/schemas/dashboards'

// Actions
import {
  notify,
  PublishNotificationAction,
} from 'src/shared/actions/notifications'
import {
  deleteTimeRange,
  updateTimeRangeFromQueryParams,
} from 'src/dashboards/actions/ranges'
import {getVariables, hydrateVariables} from 'src/variables/actions/thunks'
import {setExportTemplate} from 'src/templates/actions/creators'
import {checkDashboardLimits} from 'src/cloud/actions/limits'
import {setCells, Action as CellAction} from 'src/cells/actions/creators'
import {setViews, Action as ViewAction} from 'src/views/actions/creators'
import {updateViewAndVariables} from 'src/views/actions/thunks'
import {setLabelOnResource} from 'src/labels/actions/creators'
import * as creators from 'src/dashboards/actions/creators'

// Utils
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {dashboardToTemplate} from 'src/shared/utils/resourceToTemplate'
import {exportVariables} from 'src/variables/utils/exportVariables'
import {getSaveableView} from 'src/timeMachine/selectors'
import {incrementCloneName} from 'src/utils/naming'
import {isLimitError} from 'src/cloud/utils/limits'
import {getOrg} from 'src/organizations/selectors'
import {getAll, getByID, getStatus} from 'src/resources/selectors'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'

// Types
import {
  Dashboard,
  GetState,
  View,
  Cell,
  DashboardTemplate,
  Label,
  RemoteDataState,
  DashboardEntities,
  ViewEntities,
  ResourceType,
  VariableEntities,
  Variable,
  LabelEntities,
} from 'src/types'
import {CellsWithViewProperties} from 'src/client'
import {arrayOfVariables} from 'src/schemas/variables'

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

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      dashboardSchema
    )

    await dispatch(
      creators.setDashboard(resp.data.id, RemoteDataState.Done, normDash)
    )

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

export const cloneDashboard = (
  dashboardID: string,
  dashboardName: string
) => async (dispatch, getState: GetState): Promise<void> => {
  try {
    const state = getState()

    const org = getOrg(state)
    const dashboards = getAll<Dashboard>(state, ResourceType.Dashboards)
    const allDashboardNames = dashboards.map(d => d.name)
    const clonedName = incrementCloneName(allDashboardNames, dashboardName)

    const getResp = await api.getDashboard({
      dashboardID,
      query: {include: 'properties'},
    })

    if (getResp.status !== 200) {
      throw new Error(getResp.data.message)
    }

    const {entities, result} = normalize<Dashboard, DashboardEntities, string>(
      getResp.data,
      dashboardSchema
    )

    const dash: Dashboard = entities.dashboards[result]
    const cells = Object.values<Cell>(entities.cells || {})

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

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      postResp.data,
      dashboardSchema
    )

    await dispatch(
      creators.setDashboard(postResp.data.id, RemoteDataState.Done, normDash)
    )

    const pendingLabels = getResp.data.labels.map(l =>
      api.postDashboardsLabel({
        dashboardID: postResp.data.id,
        data: {labelID: l.id},
      })
    )

    const mappedLabels = await Promise.all(pendingLabels)

    if (mappedLabels.length > 0 && mappedLabels.some(l => l.status !== 201)) {
      throw new Error('An error occurred cloning the labels for this dashboard')
    }

    const clonedDashboardID = postResp.data.id

    const clonedViews = await dashAPI.cloneUtilFunc(
      cells,
      dashboardID,
      clonedDashboardID
    )

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

type FullAction = Action | CellAction | ViewAction

export const getDashboards = () => async (
  dispatch: Dispatch<FullAction>,
  getState: GetState
): Promise<void> => {
  try {
    const {setDashboards} = creators

    const state = getState()
    if (
      getStatus(state, ResourceType.Dashboards) === RemoteDataState.NotStarted
    ) {
      dispatch(setDashboards(RemoteDataState.Loading))
    }

    const org = getOrg(state)

    const resp = await api.getDashboards({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const dashboards = normalize<Dashboard, DashboardEntities, string[]>(
      resp.data.dashboards,
      arrayOfDashboards
    )

    dispatch(setDashboards(RemoteDataState.Done, dashboards))

    if (!dashboards.result.length) {
      return
    }

    Object.values(dashboards.entities.dashboards)
      .map(dashboard => {
        return {
          id: dashboard.id,
          cells: dashboard.cells.map(cell => dashboards.entities.cells[cell]),
        }
      })
      .forEach(entity => {
        setTimeout(() => {
          const viewsData = viewsFromCells(entity.cells, entity.id)

          const normViews = normalize<View, ViewEntities, string[]>(
            viewsData,
            arrayOfViews
          )

          dispatch(setViews(RemoteDataState.Done, normViews))
        }, 0)

        setTimeout(() => {
          const normCells = normalize<Dashboard, DashboardEntities, string[]>(
            entity.cells,
            arrayOfCells
          )

          dispatch(setCells(entity.id, RemoteDataState.Done, normCells))
        }, 0)
      })
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
      arrayOfDashboards
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

export const deleteDashboard = (dashboardID: string, name: string) => async (
  dispatch
): Promise<void> => {
  dispatch(creators.removeDashboard(dashboardID))
  dispatch(deleteTimeRange(dashboardID))

  try {
    const resp = await api.deleteDashboard({dashboardID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(notify(copy.dashboardDeleted(name)))
    dispatch(checkDashboardLimits())
  } catch (error) {
    dispatch(notify(copy.dashboardDeleteFailed(name, error.message)))
  }
}

export const getDashboard = (dashboardID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(creators.setDashboard(dashboardID, RemoteDataState.Loading))

    // Fetch the dashboard, views, and all variables a user has access to
    const [resp] = await Promise.all([
      api.getDashboard({dashboardID, query: {include: 'properties'}}),
      dispatch(getVariables()),
    ])

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const skipCache = true
    dispatch(hydrateVariables(skipCache))

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      dashboardSchema
    )

    const cellViews: CellsWithViewProperties = resp.data.cells || []
    const viewsData = viewsFromCells(cellViews, dashboardID)

    const normViews = normalize<View, ViewEntities, string[]>(
      viewsData,
      arrayOfViews
    )

    dispatch(setViews(RemoteDataState.Done, normViews))

    // Now that all the necessary state has been loaded, set the dashboard
    dispatch(creators.setDashboard(dashboardID, RemoteDataState.Done, normDash))
    dispatch(updateTimeRangeFromQueryParams(dashboardID))
  } catch (error) {
    const org = getOrg(getState())
    dispatch(push(`/orgs/${org.id}/dashboards`))
    dispatch(notify(copy.dashboardGetFailed(dashboardID, error.message)))
    return
  }
}

export const updateDashboard = (
  id: string,
  updates: Partial<Dashboard>
) => async (
  dispatch: Dispatch<creators.Action | PublishNotificationAction>,
  getState: GetState
): Promise<void> => {
  const state = getState()

  const currentDashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    id
  )

  const dashboard = {...currentDashboard, ...updates}

  try {
    const resp = await api.patchDashboard({
      dashboardID: dashboard.id,
      data: {
        name: dashboard.name,
        description: dashboard.description,
      },
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedDashboard = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      dashboardSchema
    )

    dispatch(creators.editDashboard(updatedDashboard))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.dashboardUpdateFailed()))
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

    const normLabel = normalize<Label, LabelEntities, string>(
      resp.data.label,
      labelSchema
    )

    dispatch(setLabelOnResource(dashboardID, normLabel))
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

export const convertToTemplate = (dashboardID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const state = getState()
    const org = getOrg(state)

    const dashResp = await api.getDashboard({dashboardID})

    if (dashResp.status !== 200) {
      throw new Error(dashResp.data.message)
    }

    const {entities, result} = normalize<Dashboard, DashboardEntities, string>(
      dashResp.data,
      dashboardSchema
    )

    const dashboard = entities.dashboards[result]
    const cells = dashboard.cells.map(cellID => entities.cells[cellID])

    const pendingViews = dashboard.cells.map(cellID =>
      dashAPI.getView(dashboardID, cellID)
    )

    const views = await Promise.all(pendingViews)
    const resp = await api.getVariables({query: {orgID: org.id}})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    let vars = []

    // dumb bug
    // https://github.com/paularmstrong/normalizr/issues/290
    if (resp.data.variables.length) {
      const normVars = normalize<Variable, VariableEntities, string>(
        resp.data.variables,
        arrayOfVariables
      )

      vars = Object.values(normVars.entities.variables)
    }

    const variables = filterUnusedVars(vars, views)
    const exportedVariables = exportVariables(variables, vars)
    const dashboardTemplate = dashboardToTemplate(
      state,
      dashboard,
      cells,
      views,
      exportedVariables
    )

    dispatch(setExportTemplate(RemoteDataState.Done, dashboardTemplate))
  } catch (error) {
    console.error(error)
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
      await dispatch(updateViewAndVariables(dashboardID, view))
    } else {
      await dispatch(createCellWithView(dashboardID, view))
    }
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.cellAddFailed(error.message)))
    throw error
  }
}
