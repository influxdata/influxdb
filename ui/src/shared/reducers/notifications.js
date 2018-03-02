import uuid from 'uuid'
export const initialState = []

export const notifications = (state = initialState, action) => {
  switch (action.type) {
    case 'PUBLISH_NOTIFICATION': {
      const notification = {
        ...action.payload.notification,
        id: uuid.v4(),
      }
      const newNotification = [notification]
      // Hacky way to add the new notifcation to the front of the list
      return [...newNotification, ...state]
    }

    case 'DISMISS_NOTIFICATION': {
      const {id} = action.payload
      return state.filter(n => n.id !== id)
    }
  }

  return state
}
