// Libraries
import {combineReducers} from 'redux'

// Types
import {NDashboard, RemoteDataState, AppState} from 'src/types'
import {
  SET_DASHBOARDS,
  DashboardActionTypes,
} from 'src/dashboards/actions/n.dashboards'

// TODO: port all these actions to normalized actions
// export enum ActionTypes {
// √  SetDashboards = 'SET_DASHBOARDS',
//   SetDashboard = 'SET_DASHBOARD',
//   RemoveDashboard = 'REMOVE_DASHBOARD',
//   DeleteDashboardFailed = 'DELETE_DASHBOARD_FAILED',
//   EditDashboard = 'EDIT_DASHBOARD',
//   RemoveCell = 'REMOVE_CELL',
//   AddDashboardLabels = 'ADD_DASHBOARD_LABELS',
//   RemoveDashboardLabels = 'REMOVE_DASHBOARD_LABELS',
// }

export interface DashboardsState {
  byID: {
    [uuid: string]: NDashboard
  }
  allIDs: string[]
  status: RemoteDataState
}

const byID = (
  state: DashboardsState['byID'] = {},
  action: DashboardActionTypes
): DashboardsState['byID'] => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      if (!action.normalized) {
        return state
      }

      return {...state, ...action.normalized.entities.dashboards}
    }

    default:
      return state
  }
}

const allIDs = (
  state: string[] = [],
  action: DashboardActionTypes
): DashboardsState['allIDs'] => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      if (!action.normalized) {
        return state
      }

      return [...action.normalized.result]
    }

    default:
      return state
  }
}

const status = (
  state: RemoteDataState = RemoteDataState.NotStarted,
  action: DashboardActionTypes
) => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      return action.status
    }

    default:
      return state
  }
}

export const getAllDashboards = ({resources}: AppState) =>
  resources.dashboards.allIDs.map(id => resources.dashboards.byID[id])

const dashboards = combineReducers({
  byID,
  allIDs,
  status,
})

export default dashboards
