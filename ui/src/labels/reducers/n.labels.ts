// Libraries
import {combineReducers} from 'redux'

// Types
import {Label, RemoteDataState} from 'src/types'
import {SET_LABELS, LabelActionTypes} from 'src/labels/actions/n.labels'

export interface LabelsState {
  byID: {
    [uuid: string]: Label
  }
  allIDs: string[]
  status: RemoteDataState
}

const byID = (state: LabelsState['byID'] = {}, action: LabelActionTypes) => {
  switch (action.type) {
    case SET_LABELS: {
      if (!action.normalized) {
        return state
      }

      return {...state, ...action.normalized.entities.labels}
    }

    default:
      return state
  }
}

const allIDs = (
  state: LabelsState['allIDs'] = [],
  action: LabelActionTypes
) => {
  switch (action.type) {
    case SET_LABELS: {
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
  state: LabelsState['status'] = RemoteDataState.NotStarted,
  action: LabelActionTypes
) => {
  switch (action.type) {
    case SET_LABELS: {
      return action.status
    }
    default:
      return state
  }
}

export default combineReducers({
  byID,
  allIDs,
  status,
})
