const initialState = {
  queryIDs: [],
}

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'ADD_QUERY': {
      const {queryID} = action.payload
      const newState = {
        queryIDs: state.queryIDs.concat(queryID),
      }

      return {...state, ...newState}
    }

    case 'DELETE_QUERY': {
      const {queryID} = action.payload
      const newState = {
        queryIDs: state.queryIDs.filter(id => id !== queryID),
      }

      return {...state, ...newState}
    }

    case 'SHOW_WRITE_FORM': {
      return {...state, showWriteForm: true}
    }

    case 'HIDE_WRITE_FORM': {
      return {...state, showWriteForm: false}
    }
  }

  return state
}
