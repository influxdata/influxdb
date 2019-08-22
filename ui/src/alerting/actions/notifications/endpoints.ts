// Libraries
import {Dispatch} from 'react'

// Types
import {
  NotificationEndpoint,
  GetState,
  Label,
  NotificationEndpointUpdate,
} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

// APIs
import * as api from 'src/client'

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
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    dispatch({
      type: 'SET_ALL_ENDPOINTS',
      status: RemoteDataState.Loading,
    })

    const {orgs} = getState()

    const resp = await api.getNotificationEndpoints({
      query: {orgID: orgs.org.id},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch({
      type: 'SET_ALL_ENDPOINTS',
      status: RemoteDataState.Done,
      endpoints: resp.data.notificationEndpoints,
    })
  } catch (e) {
    console.error(e)
    dispatch({type: 'SET_ALL_ENDPOINTS', status: RemoteDataState.Error})
  }
}

export const createEndpoint = (data: NotificationEndpoint) => async (
  dispatch: Dispatch<Action>
) => {
  const resp = await api.postNotificationEndpoint({data})

  if (resp.status !== 201) {
    throw new Error(resp.data.message)
  }

  dispatch({
    type: 'SET_ENDPOINT',
    endpoint: resp.data,
  })
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
}

export const deleteEndpoint = (endpointID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  const resp = await api.deleteNotificationEndpoint({endpointID})

  if (resp.status !== 204) {
    throw new Error(resp.data.message)
  }

  dispatch({
    type: 'REMOVE_ENDPOINT',
    endpointID,
  })
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
