// Libraries
import {combineReducers} from 'redux'

// Types
import {Cell} from 'src/types'
import {
  DashboardActionTypes,
  SET_DASHBOARDS,
} from 'src/dashboards/actions/n.dashboards'

export interface CellsState {
  byID: {
    [uuid: string]: Cell
  }
}

const byID = (state: CellsState['byID'] = {}, action: DashboardActionTypes) => {
  switch (action.type) {
    case SET_DASHBOARDS: {
      if (!action.normalized) {
        return state
      }

      return action.normalized.entities.cells
    }

    default:
      return state
  }
}

export default combineReducers({byID})
