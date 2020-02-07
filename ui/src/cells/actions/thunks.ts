// Libraries
import {normalize} from 'normalizr'

// APIs
import * as api from 'src/client'
import * as dashAPI from 'src/dashboards/apis'

// Schemas
import {
  dashboardSchema,
  cellSchema,
  arrayOfCells,
  viewSchema,
} from 'src/schemas'

// Actions
import {refreshDashboardVariableValues} from 'src/dashboards/actions/thunks'
import {setView} from 'src/views/actions/creators'
import {notify} from 'src/shared/actions/notifications'
import {setCells, setCell, removeCell} from 'src/cells/actions/creators'

// Utils
import {getClonedDashboardCell} from 'src/dashboards/utils/cellGetters'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {
  Dashboard,
  NewView,
  Cell,
  GetState,
  RemoteDataState,
  NewCell,
  DashboardEntities,
  ResourceType,
  CellEntities,
  View,
  ViewEntities,
} from 'src/types'

// Utils
import {getViewsForDashboard} from 'src/views/selectors'
import {getNewDashboardCell} from 'src/dashboards/utils/cellGetters'
import {getByID} from 'src/resources/selectors'

export const deleteCell = (dashboardID: string, cellID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const views = getViewsForDashboard(getState(), dashboardID).filter(
      view => view.cellID !== cellID
    )

    await Promise.all([
      api.deleteDashboardsCell({dashboardID: dashboardID, cellID: cellID}),
      dispatch(refreshDashboardVariableValues(dashboardID, views)),
    ])

    dispatch(removeCell({dashboardID, id: cellID}))
    dispatch(notify(copy.cellDeleted()))
  } catch (error) {
    console.error(error)
  }
}

export const createCellWithView = (
  dashboardID: string,
  view: NewView,
  clonedCell?: Cell
) => async (dispatch, getState: GetState): Promise<void> => {
  const state = getState()

  let dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    dashboardID
  )

  try {
    if (!dashboard) {
      const resp = await api.getDashboard({dashboardID})
      if (resp.status !== 200) {
        throw new Error(resp.data.message)
      }

      const {entities, result} = normalize<
        Dashboard,
        DashboardEntities,
        string
      >(resp.data, dashboardSchema)

      dashboard = entities.dashboards[result]
    }

    const cell: NewCell = getNewDashboardCell(state, dashboard, clonedCell)

    // Create the cell
    const cellResp = await api.postDashboardsCell({dashboardID, data: cell})

    if (cellResp.status !== 201) {
      throw new Error(cellResp.data.message)
    }

    const cellID = cellResp.data.id

    // Create the view and associate it with the cell
    const newView = await dashAPI.updateView(dashboardID, cellID, view)

    const normCell = normalize<Cell, CellEntities, string>(
      {...cellResp.data, dashboardID},
      cellSchema
    )

    // Refresh variables in use on dashboard
    const views = [...getViewsForDashboard(state, dashboardID), newView]

    await dispatch(refreshDashboardVariableValues(dashboardID, views))

    const normView = normalize<View, ViewEntities, string>(newView, viewSchema)

    dispatch(setView(cellID, RemoteDataState.Done, normView))
    dispatch(setCell(cellID, RemoteDataState.Done, normCell))
  } catch {
    notify(copy.cellAddFailed())
  }
}

export const updateCells = (dashboardID: string, cells: Cell[]) => async (
  dispatch
): Promise<void> => {
  try {
    const resp = await api.putDashboardsCells({
      dashboardID,
      data: cells,
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const updatedCells = cells.map(c => ({...c, dashboardID}))

    const normCells = normalize<Dashboard, DashboardEntities, string[]>(
      updatedCells,
      arrayOfCells
    )

    dispatch(setCells(dashboardID, RemoteDataState.Done, normCells))
  } catch (error) {
    console.error(error)
  }
}

export const copyCell = (dashboard: Dashboard, cell: Cell) => dispatch => {
  try {
    const clonedCell = getClonedDashboardCell(dashboard, cell)

    const normCell = normalize<Dashboard, DashboardEntities, string>(
      clonedCell,
      cellSchema
    )

    dispatch(setCell(cell.id, RemoteDataState.Done, normCell))
    dispatch(notify(copy.cellAdded()))
  } catch (error) {
    console.error(error)
  }
}
