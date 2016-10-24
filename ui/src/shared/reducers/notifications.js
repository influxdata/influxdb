import u from 'updeep';

export default function notifications(state = {}, action) {
  switch (action.type) {
    case 'NOTIFICATION_RECEIVED': {
      const {type, message} = action.payload;
      return u.updateIn(type, message, state);
    }
    case 'NOTIFICATION_DISMISSED': {
      const {type} = action.payload;
      return u(u.omit(type), state);
    }
  }

  return state;
}
