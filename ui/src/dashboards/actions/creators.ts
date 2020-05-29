// Types
import {RemoteDataState, DashboardEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'
import {setLabelOnResource} from 'src/labels/actions/creators'

export const ADD_DASHBOARD_LABEL = 'ADD_DASHBOARD_LABEL'
export const DELETE_DASHBOARD_FAILED = 'DELETE_DASHBOARD_FAILED'
export const EDIT_DASHBOARD = 'EDIT_DASHBOARD'
export const REMOVE_DASHBOARD = 'REMOVE_DASHBOARD'
export const REMOVE_DASHBOARD_LABEL = 'REMOVE_DASHBOARD_LABEL'
export const SET_DASHBOARD = 'SET_DASHBOARD'
export const SET_DASHBOARDS = 'SET_DASHBOARDS'

export type Action =
  | ReturnType<typeof editDashboard>
  | ReturnType<typeof removeDashboard>
  | ReturnType<typeof removeDashboardLabel>
  | ReturnType<typeof setDashboard>
  | ReturnType<typeof setDashboards>
  | ReturnType<typeof setLabelOnResource>

// R is the type of the value of the "result" key in normalizr's normalization
type DashboardSchema<R extends string | string[]> = NormalizedSchema<
  DashboardEntities,
  R
>

// Action Creators
export const editDashboard = (schema: DashboardSchema<string>) =>
  ({
    type: EDIT_DASHBOARD,
    schema,
  } as const)

export const setDashboards = (
  status: RemoteDataState,
  schema?: DashboardSchema<string[]>
) =>
  ({
    type: SET_DASHBOARDS,
    status,
    schema,
  } as const)

export const setDashboard = (
  id: string,
  status: RemoteDataState,
  schema?: DashboardSchema<string>
) =>
  ({
    type: SET_DASHBOARD,
    id,
    status,
    schema,
  } as const)

export const removeDashboard = (id: string) =>
  ({
    type: REMOVE_DASHBOARD,
    id,
  } as const)

export const removeDashboardLabel = (dashboardID: string, labelID: string) =>
  ({
    type: REMOVE_DASHBOARD_LABEL,
    dashboardID,
    labelID,
  } as const)
