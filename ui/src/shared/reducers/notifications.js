import u from 'updeep'
import _ from 'lodash'

export const notifications = (state = {}, action) => {
  switch (action.type) {
    case 'NOTIFICATION_RECEIVED': {
      const {type, message} = action.payload
      return u.updateIn(type, message, state)
    }
    case 'NOTIFICATION_DISMISSED': {
      const {type} = action.payload
      return u(u.omit(type), state)
    }
    case 'ALL_NOTIFICATIONS_DISMISSED': {
      // Reset to initial state
      return {}
    }
  }

  return state
}

export const getNotificationID = (message, type) => _.snakeCase(message) + type

export const dismissedNotifications = (state = {}, action) => {
  switch (action.type) {
    case 'NOTIFICATION_RECEIVED': {
      const {type, message, once} = action.payload
      if (once) {
        // Create a message ID in a deterministic way, also with its type
        const messageID = getNotificationID(message, type)
        if (state[messageID]) {
          // Message action called with once option but we've already seen it
          return state
        }
        // Message action called with once option and it's not present on
        // the persisted state
        return {
          ...state,
          [messageID]: true,
        }
      }
      // Message action not called with once option
      return state
    }
  }

  return state
}
