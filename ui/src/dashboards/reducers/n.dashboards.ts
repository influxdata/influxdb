// Libraries
import {combineReducers} from 'redux'

// Types
import {NDashboard, RemoteDataState} from 'src/types'

export const SET_DASHBOARDS = 'N_SET_DASHBOARDS'

interface SetDashboardsAction {
  type: typeof SET_DASHBOARDS
}

export type Actions = SetDashboardsAction

export interface DashboardsState {
  byID: {
    [uuid: string]: NDashboard
  }
  allIDs: string[]
  status: RemoteDataState
}

const byID = (
  state: DashboardsState['byID'] = {},
  action: Actions
): DashboardsState['byID'] => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      return state
    }

    default:
      return state
  }
}

const allIDs = (
  state: string[] = [],
  action: Actions
): DashboardsState['allIDs'] => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      return state
    }

    default:
      return state
  }
}

// const getAllDashboards = state => state.allIDs.map(id => state.byID[id])

const status = (
  state: RemoteDataState = RemoteDataState.NotStarted,
  action: Actions
) => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      return state
    }

    default:
      return state
  }
}

const dashboards = combineReducers({
  byID,
  allIDs,
  status,
})

export default dashboards
