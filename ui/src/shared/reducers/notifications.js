import uuid from 'uuid'
export const initialState = []

export const notifications = (state = initialState, action) => {
  switch (action.type) {
    case 'PUBLISH_NOTIFICATION': {
      const notification = {
        ...action.payload.notification,
        id: uuid.v4(),
        created: Date.now(),
        dismiss: false,
      }
      const newNotification = [notification]
      // Hacky way to add the new notifcation to the front of the list
      return [...newNotification, ...state]
    }

    case 'DISMISS_NOTIFICATION': {
      const {notificationID} = action.payload

      return state.map(n => {
        return n.id === notificationID ? {...n, dismiss: true} : n
      })
    }

    case 'DELETE_NOTIFICATION': {
      return state.filter(n => n.id === action.payload)
    }
  }

  return state
}
