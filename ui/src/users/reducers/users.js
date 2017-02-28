export default function users(state = {}, action) {
  switch (action.type) {
    case 'LOAD_USERS': {
      return {...state, ...action.payload}
    }
  }
  return state
}
