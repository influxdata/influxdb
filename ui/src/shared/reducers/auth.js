function getInitialState() {
  return [];
}
const initialState = getInitialState();

export default function auth(state = initialState, action) {
  switch (action.type) {
    case 'AUTH_RECEIVED': {
      return action.payload.auth;
    }
  }

  return state;
}
