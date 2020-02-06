// Libraries
import {produce} from 'immer'

// Types
import {Check, RemoteDataState, ResourceState, ResourceType} from 'src/types'
import {
  Action,
  SET_CHECKS,
  SET_CHECK,
  REMOVE_CHECK,
  REMOVE_LABEL_FROM_CHECK,
} from 'src/checks/actions/creators'

import {SET_LABEL_ON_RESOURCE} from 'src/labels/actions/creators'

import {
  setResource,
  setResourceAtID,
  removeResource,
  setRelation,
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

      case SET_LABEL_ON_RESOURCE: {
        const {resourceID, schema} = action
        const labelID = schema.result

        setRelation<Check>(draftState, ResourceType.Labels, labelID, resourceID)

        return
      }

      case REMOVE_LABEL_FROM_CHECK: {
        const {checkID, labelID} = action
        const labels = draftState.byID[checkID].labels
        draftState.byID[checkID].labels = labels.filter(id => id !== labelID)

        return
      }
    }
  })
