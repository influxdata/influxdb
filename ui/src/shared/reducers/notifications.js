export default function notifications(state = {}, action) {
  switch (action.type) {
    case 'NOTIFICATION_RECEIVED': {
      const {type, message} = action.payload;
      return Object.assign({}, state, {
        [type]: message,
      });
    }
    case 'NOTIFICATION_DISMISSED': {
      const {type} = action.payload;
      return Object.assign({}, state, {
        [type]: undefined,
      });
    }
  }

  return state;
}
