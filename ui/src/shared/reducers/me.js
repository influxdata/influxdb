function getInitialState() {
  return {};
}
const initialState = getInitialState();

export default function me(state = initialState, action) {
  switch (action.type) {
    case 'ME_RECEIVED': {
      return action.payload.me;
    }
    case 'LOGOUT': {
      return {};
    }
  }

  return state;
}
