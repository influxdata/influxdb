// Libraries
import {normalize} from 'normalizr'

// APIs
import {
  getDashboard,
  deleteDashboardsCell,
  postDashboard,
  postDashboardsCell,
  putDashboardsCells,
} from 'src/client'
import {updateView} from 'src/dashboards/apis'

// Schemas
import {
  dashboardSchema,
  cellSchema,
  arrayOfCells,
  viewSchema,
} from 'src/schemas'

// Actions
import {setView} from 'src/views/actions/creators'
import {notify} from 'src/shared/actions/notifications'
import {setDashboard} from 'src/dashboards/actions/creators'
import {setCells, setCell, removeCell} from 'src/cells/actions/creators'

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
import {getNewDashboardCell} from 'src/dashboards/utils/cellGetters'
import {getByID} from 'src/resources/selectors'

export const deleteCell = (dashboardID: string, cellID: string) => async (
  dispatch
): Promise<void> => {
  try {
    await Promise.all([
      deleteDashboardsCell({dashboardID: dashboardID, cellID: cellID}),
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
      const resp = await getDashboard({dashboardID})
      if (resp.status !== 200) {
        throw new Error(resp.data.message)
      }

      const normDash = normalize<Dashboard, DashboardEntities, string>(
        resp.data,
        dashboardSchema
      )
      const {entities, result} = normDash

      dashboard = entities.dashboards[result]
      dispatch(setDashboard(resp.data.id, RemoteDataState.Done, normDash))
    }

    const cell: NewCell = getNewDashboardCell(state, dashboard, clonedCell)

    // Create the cell
    const cellResp = await postDashboardsCell({dashboardID, data: cell})

    if (cellResp.status !== 201) {
      throw new Error(cellResp.data.message)
    }

    const cellID = cellResp.data.id

    // Create the view and associate it with the cell
    const newView = await updateView(dashboardID, cellID, view)

    const normCell = normalize<Cell, CellEntities, string>(
      {...cellResp.data, dashboardID},
      cellSchema
    )

    // Refresh variables in use on dashboard
    const normView = normalize<View, ViewEntities, string>(newView, viewSchema)

    dispatch(setCell(cellID, RemoteDataState.Done, normCell))
    dispatch(setView(cellID, RemoteDataState.Done, normView))
  } catch (error) {
    dispatch(notify(copy.cellAddFailed(error.message)))
    throw error
  }
}

export const createDashboardWithView = (
  orgID: string,
  dashboardName: string,
  view: View
) => async (dispatch): Promise<void> => {
  try {
    const newDashboard = {
      orgID,
      name: dashboardName,
      cells: [],
    }

    const resp = await postDashboard({data: newDashboard})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const normDash = normalize<Dashboard, DashboardEntities, string>(
      resp.data,
      dashboardSchema
    )

    await dispatch(setDashboard(resp.data.id, RemoteDataState.Done, normDash))

    await dispatch(createCellWithView(resp.data.id, view))
    dispatch(notify(copy.dashboardCreateSuccess()))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.cellAddFailed(error.message)))
    throw error
  }
}

export const updateCells = (dashboardID: string, cells: Cell[]) => async (
  dispatch
): Promise<void> => {
  try {
    const resp = await putDashboardsCells({
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
    dispatch(notify(copy.cellUpdateFailed()))
    console.error(error)
  }
}
