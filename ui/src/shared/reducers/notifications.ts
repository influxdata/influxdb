import uuid from 'uuid'
import {Action, ActionTypes} from 'src/shared/actions/notifications'
import {Notification} from 'src/types'

export const initialState: Notification[] = []

export const notifications = (state = initialState, action: Action) => {
  switch (action.type) {
    case ActionTypes.PUBLISH_NOTIFICATION: {
      const {notification} = action.payload
      const publishedNotification = {
        ...notification,
        id: uuid.v4(),
      }

      return [publishedNotification, ...state]
    }

    case ActionTypes.DISMISS_NOTIFICATION: {
      const {id} = action.payload
      return state.filter(n => n.id !== id)
    }

    case ActionTypes.DISMISS_ALL_NOTIFICATIONS: {
      return []
    }
  }

  return state
}
