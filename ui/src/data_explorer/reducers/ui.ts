interface DataExplorerState {
  queryIDs: ReadonlyArray<string>
}

const initialState = {
  queryIDs: [],
}

const ui = (
  state: DataExplorerState = initialState,
  action
): DataExplorerState => {
  switch (action.type) {
    // there is an additional reducer for this same action in the queryConfig reducer
    case 'DE_ADD_QUERY': {
      const {queryID} = action.payload
      const newState = {
        queryIDs: state.queryIDs.concat(queryID),
      }

      return {...state, ...newState}
    }

    // there is an additional reducer for this same action in the queryConfig reducer
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

export default ui
