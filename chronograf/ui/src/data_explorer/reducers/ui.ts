interface DataExplorerState {
  queryIDs: ReadonlyArray<string>
}

interface ActionAddQuery {
  type: 'DE_ADD_QUERY'
  payload: {
    queryID: string
  }
}

interface ActionDeleteQuery {
  type: 'DE_DELETE_QUERY'
  payload: {
    queryID: string
  }
}

type Action = ActionAddQuery | ActionDeleteQuery

const initialState = {
  queryIDs: [],
}

const ui = (
  state: DataExplorerState = initialState,
  action: Action
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
