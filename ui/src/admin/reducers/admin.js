import reject from 'lodash/reject'

const initialState = {
  users: [],
  roles: [],
  queries: [],
  queryIDToKill: null,
}

export default function admin(state = initialState, action) {
  switch (action.type) {
    case 'LOAD_USERS': {
      return {...state, ...action.payload}
    }

    case 'LOAD_ROLES': {
      return {...state, ...action.payload}
    }

    case 'ADD_USER': {
      const newState = Object.assign({}, state, {
        users: [
          action.payload.user,
          ...state.users,
        ]})
      return newState
    }

    case 'LOAD_QUERIES': {
      return {...state, ...action.payload}
    }

    case 'KILL_QUERY': {
      const {queryID} = action.payload
      const nextState = {
        queries: reject(state.queries, (q) => +q.id === +queryID),
      }

      return {...state, ...nextState}
    }

    case 'SET_QUERY_TO_KILL': {
      return {...state, ...action.payload}
    }
  }

  return state
}
