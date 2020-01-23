// Libraries
import {Dispatch} from 'react'

// Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'
import {checkEndpointsLimits} from 'src/cloud/actions/limits'

// APIs
import * as api from 'src/client'

// Utils
import {incrementCloneName} from 'src/utils/naming'
import {getOrg} from 'src/organizations/selectors'
import * as copy from 'src/shared/copy/notifications'

// Types
import {
  NotificationEndpoint,
  GetState,
  Label,
  NotificationEndpointUpdate,
  PostNotificationEndpoint,
} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

export type Action =
  | {type: 'SET_ENDPOINT'; endpoint: NotificationEndpoint}
  | {type: 'REMOVE_ENDPOINT'; endpointID: string}
  | {
      type: 'SET_ALL_ENDPOINTS'
      status: RemoteDataState
      endpoints?: NotificationEndpoint[]
    }
  | {
      type: 'ADD_LABEL_TO_ENDPOINT'
      endpointID: string
      label: Label
    }
  | {
      type: 'REMOVE_LABEL_FROM_ENDPOINT'
      endpointID: string
      label: Label
    }

export const getEndpoints = () => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >,
  getState: GetState
) => {
  try {
    dispatch({
      type: 'SET_ALL_ENDPOINTS',
      status: RemoteDataState.Loading,
    })

    const {id: orgID} = getOrg(getState())

    const resp = await api.getNotificationEndpoints({
      query: {orgID},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch({
      type: 'SET_ALL_ENDPOINTS',
      status: RemoteDataState.Done,
      endpoints: resp.data.notificationEndpoints,
    })
    dispatch(checkEndpointsLimits())
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.getEndpointsFailed(e.message)))
    dispatch({type: 'SET_ALL_ENDPOINTS', status: RemoteDataState.Error})
  }
}

export const createEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >
) => {
  const labels = endpoint.labels || []

  const data = {
    ...endpoint,
    labels: labels.map(l => l.id),
  } as PostNotificationEndpoint

  const resp = await api.postNotificationEndpoint({data})

  if (resp.status !== 201) {
    throw new Error(resp.data.message)
  }

  dispatch({
    type: 'SET_ENDPOINT',
    endpoint: resp.data,
  })
  dispatch(checkEndpointsLimits())
}

export const updateEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  const resp = await api.putNotificationEndpoint({
    endpointID: endpoint.id,
    data: endpoint,
  })

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  dispatch({
    type: 'SET_ENDPOINT',
    endpoint: resp.data,
  })
}

export const updateEndpointProperties = (
  endpointID: string,
  properties: NotificationEndpointUpdate
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.patchNotificationEndpoint({
      endpointID,
      data: properties,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch({
      type: 'SET_ENDPOINT',
      endpoint: resp.data,
    })
  } catch (e) {
    dispatch(notify(copy.updateEndpointFailed(e.message)))
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

    dispatch({
      type: 'REMOVE_ENDPOINT',
      endpointID,
    })
    dispatch(checkEndpointsLimits())
  } catch (e) {
    dispatch(notify(copy.deleteEndpointFailed(e.message)))
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

    dispatch({
      type: 'ADD_LABEL_TO_ENDPOINT',
      endpointID,
      label,
    })
  } catch (e) {
    console.error(e)
  }
}

export const deleteEndpointLabel = (endpointID: string, label: Label) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteNotificationEndpointsLabel({
      endpointID,
      labelID: label.id,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch({
      type: 'REMOVE_LABEL_FROM_ENDPOINT',
      endpointID,
      label,
    })
  } catch (e) {
    console.error(e)
  }
}

export const cloneEndpoint = (endpoint: NotificationEndpoint) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkEndpointsLimits>
  >,
  getState: GetState
): Promise<void> => {
  try {
    const {
      endpoints: {list},
    } = getState()

    const allEndpointNames = list.map(r => r.name)

    const clonedName = incrementCloneName(allEndpointNames, endpoint.name)

    const labels = endpoint.labels || []

    const resp = await api.postNotificationEndpoint({
      data: {
        ...endpoint,
        name: clonedName,
        labels: labels.map(l => l.id),
      } as PostNotificationEndpoint,
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch({type: 'SET_ENDPOINT', endpoint: resp.data})
    dispatch(checkEndpointsLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createEndpointFailed(error.message)))
  }
}
