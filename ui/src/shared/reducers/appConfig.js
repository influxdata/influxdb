const initialState = {
  autoRefreshMs: 15000,
};

export default function appConfig(state = initialState, action) {
  switch (action.type) {
    case 'SET_AUTOREFRESH': {
      return {
        ...state,
        autoRefreshMs: action.payload,
      }
    }

    // TODO implement 'GET_AUTOREFRESH'
  }

  return state
}
