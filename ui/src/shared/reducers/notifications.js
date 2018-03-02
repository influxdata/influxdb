import uuid from 'uuid'

export const initialState = []

export const notifications = (state = initialState, action) => {
  switch (action.type) {
    case 'PUBLISH_NOTIFICATION': {
      const newNotification = {
        ...action.payload,
        id: uuid.v4(),
        created: Date.now(),
      }

      return [...state, newNotification]
    }

    case 'DISMISS_NOTIFICATION': {
      return state.filter(n => n.id === action.payload)
    }
  }

  return state
}
