// Libraries
import {Dispatch} from 'react'

// Types
import {NotificationEndpoint} from 'src/types'

// APIs
import * as api from 'src/client'

export type Action = {type: 'SET_ENDPOINT'; endpoint: NotificationEndpoint}

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
