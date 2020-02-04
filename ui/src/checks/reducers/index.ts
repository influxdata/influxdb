// Libraries
import {produce} from 'immer'

// Types
import {Check, RemoteDataState, ResourceState, ResourceType} from 'src/types'
import {
  Action,
  SET_CHECKS,
  SET_CHECK,
  REMOVE_CHECK,
  ADD_LABEL_TO_CHECK,
  REMOVE_LABEL_FROM_CHECK,
} from 'src/checks/actions/creators'
import {
  setResource,
  setResourceAtID,
  removeResource,
} from 'src/resources/reducers/helpers'

export type ChecksState = ResourceState['checks']

export const defaultChecksState: ChecksState = {
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
}

export interface ResourceIDs {
  checkIDs: {[x: string]: boolean}
  endpointIDs: {[x: string]: boolean}
  ruleIDs: {[x: string]: boolean}
}

export default (
  state: ChecksState = defaultChecksState,
  action: Action
): ChecksState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_CHECKS: {
        setResource<Check>(draftState, action, ResourceType.Checks)

        return
      }

      case SET_CHECK: {
        setResourceAtID<Check>(draftState, action, ResourceType.Checks)

        return
      }

      case REMOVE_CHECK: {
        removeResource<Check>(draftState, action)

        return
      }

      case ADD_LABEL_TO_CHECK: {
        const {checkID, label} = action

        const labels = draftState.byID[checkID].labels
        draftState.byID[checkID].labels = [...labels, label]

        return
      }

      case REMOVE_LABEL_FROM_CHECK: {
        const {checkID, labelID} = action
        const labels = draftState.byID[checkID].labels
        draftState.byID[checkID].labels = labels.filter(l => l.id !== labelID)

        return
      }
    }
  })
