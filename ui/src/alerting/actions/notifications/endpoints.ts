// Libraries
import {Dispatch} from 'react'

// Types
import {NotificationEndpoint, GetState} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

// APIs
import * as api from 'src/client'

export type Action =
  | {type: 'SET_ENDPOINT'; endpoint: NotificationEndpoint}
  | {
      type: 'SET_ALL_ENDPOINTS'
      status: RemoteDataState
      endpoints?: NotificationEndpoint[]
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

  const endpoint = (resp.data as unknown) as NotificationEndpoint

  dispatch({
    type: 'SET_ENDPOINT',
    endpoint,
  })
}
