// Libraries
import {Dispatch} from 'react'
import {normalize} from 'normalizr'

// Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'
import {checkEndpointsLimits} from 'src/cloud/actions/limits'
import {
  setEndpoints,
  setEndpoint,
  Action,
  removeEndpoint,
  removeLabelFromEndpoint,
} from 'src/notifications/endpoints/actions/creators'
import {setLabelOnResource} from 'src/labels/actions/creators'

// Schemas
import {endpointSchema, arrayOfEndpoints} from 'src/schemas/endpoints'
import {labelSchema} from 'src/schemas/labels'

// APIs
import * as api from 'src/client'

// Utils
import {incrementCloneName} from 'src/utils/naming'
import {getOrg} from 'src/organizations/selectors'
import {getAll, getStatus} from 'src/resources/selectors'
import {toPostNotificationEndpoint} from 'src/notifications/endpoints/utils'
import * as copy from 'src/shared/copy/notifications'

// Types
import {
  NotificationEndpoint,
  GetState,
  Label,
  NotificationEndpointUpdate,
  RemoteDataState,
  EndpointEntities,
  ResourceType,
  LabelEntities,
} from 'src/types'

export const getEndpoints = () => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >,
  getState: GetState
) => {
  try {
    const state = getState()
    if (
      getStatus(state, ResourceType.NotificationEndpoints) ===
      RemoteDataState.NotStarted
    ) {
      dispatch(setEndpoints(RemoteDataState.Loading))
    }

    const {id: orgID} = getOrg(state)

    const resp = await api.getNotificationEndpoints({
      query: {orgID},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const endpoints = normalize<
      NotificationEndpoint,
      EndpointEntities,
      string[]
    >(resp.data.notificationEndpoints, arrayOfEndpoints)

    dispatch(setEndpoints(RemoteDataState.Done, endpoints))
    dispatch(checkEndpointsLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.getEndpointsFailed(error.message)))
    dispatch(setEndpoints(RemoteDataState.Error))
  }
}

export const createEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >
) => {
  const data = toPostNotificationEndpoint(endpoint)

  try {
    const resp = await api.postNotificationEndpoint({data})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const newEndpoint = normalize<
      NotificationEndpoint,
      EndpointEntities,
      string
    >(resp.data, endpointSchema)

    dispatch(setEndpoint(resp.data.id, RemoteDataState.Done, newEndpoint))
    dispatch(checkEndpointsLimits())
  } catch (error) {
    console.error(error)
  }
}

export const updateEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  dispatch(setEndpoint(endpoint.id, RemoteDataState.Loading))
  const data = toPostNotificationEndpoint(endpoint)

  try {
    const resp = await api.putNotificationEndpoint({
      endpointID: endpoint.id,
      data,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updates = normalize<NotificationEndpoint, EndpointEntities, string>(
      resp.data,
      endpointSchema
    )

    dispatch(setEndpoint(endpoint.id, RemoteDataState.Done, updates))
  } catch (error) {
    console.error(error)
    dispatch(setEndpoint(endpoint.id, RemoteDataState.Error))
  }
}

export const updateEndpointProperties = (
  endpointID: string,
  properties: NotificationEndpointUpdate
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  dispatch(setEndpoint(endpointID, RemoteDataState.Loading))
  try {
    const resp = await api.patchNotificationEndpoint({
      endpointID,
      data: properties,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updates = normalize<NotificationEndpoint, EndpointEntities, string>(
      resp.data,
      endpointSchema
    )

    dispatch(setEndpoint(endpointID, RemoteDataState.Done, updates))
  } catch (error) {
    dispatch(notify(copy.updateEndpointFailed(error.message)))
    dispatch(setEndpoint(endpointID, RemoteDataState.Error))
  }
}

export const deleteEndpoint = (endpointID: string) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >
) => {
  try {
    const resp = await api.deleteNotificationEndpoint({endpointID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeEndpoint(endpointID))
    dispatch(checkEndpointsLimits())
  } catch (error) {
    dispatch(notify(copy.deleteEndpointFailed(error.message)))
  }
}

export const addEndpointLabel = (endpointID: string, label: Label) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.postNotificationEndpointsLabel({
      endpointID,
      data: {labelID: label.id},
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const normLabel = normalize<Label, LabelEntities, string>(
      resp.data.label,
      labelSchema
    )

    dispatch(setLabelOnResource(endpointID, normLabel))
  } catch (error) {
    console.error(error)
  }
}

export const deleteEndpointLabel = (
  endpointID: string,
  labelID: string
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.deleteNotificationEndpointsLabel({
      endpointID,
      labelID,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeLabelFromEndpoint(endpointID, labelID))
  } catch (error) {
    console.error(error)
  }
}

export const cloneEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >,
  getState: GetState
): Promise<void> => {
  try {
    const state = getState()
    const endpoints = getAll<NotificationEndpoint>(
      state,
      ResourceType.NotificationEndpoints
    )

    const allEndpointNames = endpoints.map(r => r.name)

    const clonedName = incrementCloneName(allEndpointNames, endpoint.name)

    const resp = await api.postNotificationEndpoint({
      data: {
        ...toPostNotificationEndpoint(endpoint),
        name: clonedName,
      },
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const clone = normalize<NotificationEndpoint, EndpointEntities, string>(
      resp.data,
      endpointSchema
    )

    dispatch(setEndpoint(resp.data.id, RemoteDataState.Done, clone))
    dispatch(checkEndpointsLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createEndpointFailed(error.message)))
  }
}
