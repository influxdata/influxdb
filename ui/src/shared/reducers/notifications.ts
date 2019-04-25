import {produce} from 'immer'
import uuid from 'uuid'
import {Action, ActionTypes} from 'src/shared/actions/notifications'
import {Notification} from 'src/types'

export const initialState: Notification[] = []

export const notificationsReducer = (
  state = initialState,
  action: Action
): Notification[] =>
  produce(state, draftState => {
    switch (action.type) {
      case ActionTypes.PUBLISH_NOTIFICATION: {
        const {notification} = action.payload
        const publishedNotification = {
          ...notification,
          id: uuid.v4(),
        }
        draftState.push(publishedNotification)

        return
      }

      case ActionTypes.DISMISS_NOTIFICATION: {
        const {id} = action.payload
        draftState = state.filter(n => n.id !== id)
        return
      }

      case ActionTypes.DISMISS_ALL_NOTIFICATIONS: {
        draftState = []
        return
      }
    }
  })
