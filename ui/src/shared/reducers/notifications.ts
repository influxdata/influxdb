import uuid from 'uuid'
import {Action} from 'src/types/actions/notifications'
import {Notification} from 'src/types'

export const initialState: Notification[] = []

export const notifications = (state = initialState, action: Action) => {
  switch (action.type) {
    case 'PUBLISH_NOTIFICATION': {
      const {notification} = action.payload
      const publishedNotification = {
        ...notification,
        id: uuid.v4(),
      }

      return [publishedNotification, ...state]
    }

    case 'DISMISS_NOTIFICATION': {
      const {id} = action.payload
      return state.filter(n => n.id !== id)
    }
  }

  return state
}
