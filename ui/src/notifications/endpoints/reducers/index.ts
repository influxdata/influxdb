// Libraries
import produce from 'immer'

// Types
import {
  ResourceState,
  NotificationEndpoint,
  RemoteDataState,
  ResourceType,
} from 'src/types'
import {
  Action,
  SET_ENDPOINTS,
  SET_ENDPOINT,
  REMOVE_ENDPOINT,
  REMOVE_LABEL_FROM_ENDPOINT,
} from 'src/notifications/endpoints/actions/creators'

import {SET_LABEL_ON_RESOURCE} from 'src/labels/actions/creators'

// Helpers
import {
  setResource,
  removeResource,
  setResourceAtID,
  setRelation,
} from 'src/resources/reducers/helpers'

type EndpointsState = ResourceState['endpoints']

const initialState = {
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
}

export default (
  state: EndpointsState = initialState,
  action: Action
): EndpointsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_ENDPOINTS: {
        setResource<NotificationEndpoint>(
          draftState,
          action,
          ResourceType.NotificationEndpoints
        )

        return
      }

      case SET_ENDPOINT: {
        setResourceAtID<NotificationEndpoint>(
          draftState,
          action,
          ResourceType.NotificationEndpoints
        )

        return
      }

      case REMOVE_ENDPOINT: {
        removeResource<NotificationEndpoint>(draftState, action)

        return
      }

      case SET_LABEL_ON_RESOURCE: {
        const {resourceID, schema} = action
        const labelID = schema.result

        setRelation<NotificationEndpoint>(
          draftState,
          ResourceType.Labels,
          labelID,
          resourceID
        )

        return
      }

      case REMOVE_LABEL_FROM_ENDPOINT: {
        const {endpointID, labelID} = action

        const labels = draftState.byID[endpointID].labels

        draftState.byID[endpointID].labels = labels.filter(id => id !== labelID)

        return
      }
    }
  })
