import u from 'updeep'

function getInitialState() {
  return {}
}
const initialState = getInitialState()

const notificationsReducer = (state = initialState, action) => {
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
      return getInitialState()
    }
  }

  return state
}

export default notificationsReducer
