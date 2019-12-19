// Libraries
import {normalize, NormalizedSchema} from 'normalizr'

// APIs
import * as api from 'src/dashboards/apis'

// Schemas
import * as schemas from 'src/schemas'

// Types
import {Dispatch} from 'redux-thunk'
import {Entities, RemoteDataState, AppThunk, GetState} from 'src/types'

export const SET_DASHBOARDS = 'N_SET_DASHBOARDS'
export const REMOVE_DASHBOARD = 'N_REMOVE_DASHBOARDS'

interface SetDashboardsAction {
  type: typeof SET_DASHBOARDS
  status: RemoteDataState
  normalized?: NormalizedSchema<Entities, string[]>
}

interface RemoveDashboardAction {
  type: typeof REMOVE_DASHBOARD
  id: string
}

export type DashboardActionTypes = SetDashboardsAction | RemoveDashboardAction

export const setDashboards = (
  status: RemoteDataState,
  normalized?: SetDashboardsAction['normalized']
): DashboardActionTypes => {
  return {
    type: SET_DASHBOARDS,
    status,
    normalized,
  }
}

export const removeDashboard = (id: string): DashboardActionTypes => {
  return {
    type: REMOVE_DASHBOARD,
    id,
  }
}

export const getDashboards = (): AppThunk => async (
  dispatch: Dispatch<DashboardActionTypes>,
  getState: GetState
): Promise<void> => {
  try {
    const {
      orgs: {org},
    } = getState()

    dispatch(setDashboards(RemoteDataState.Loading))
    const dashboards = await api.getDashboards(org.id)
    const normalized = normalize<any, Entities, string[]>(dashboards, [
      schemas.dashboards,
    ])

    console.log('normalized dashbaords: ', normalized)

    dispatch(setDashboards(RemoteDataState.Done, normalized))
  } catch (error) {
    dispatch(setDashboards(RemoteDataState.Error))
    console.error(error)
    throw error
  }
}
