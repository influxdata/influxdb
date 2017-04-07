import u from 'updeep'

function getInitialState() {
  return {}
}
const initialState = getInitialState()

export default function notifications(state = initialState, action) {
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
