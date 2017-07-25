const initialState = {
  queryIDs: [],
}

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'DE_ADD_QUERY': {
      const {queryID} = action.payload
      const newState = {
        queryIDs: state.queryIDs.concat(queryID),
      }

      return {...state, ...newState}
    }

    case 'DE_DELETE_QUERY': {
      const {queryID} = action.payload
      const newState = {
        queryIDs: state.queryIDs.filter(id => id !== queryID),
      }

      return {...state, ...newState}
    }
  }

  return state
}
