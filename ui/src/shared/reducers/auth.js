const getInitialState = () => ({
  links: null,
  me: null,
  isMeLoading: false,
  isAuthLoading: false,
})

const initialState = getInitialState()

const auth = (state = initialState, action) => {
  switch (action.type) {
    case 'AUTH_REQUESTED': {
      return {...state, isAuthLoading: true}
    }
    case 'AUTH_RECEIVED': {
      const links = action.payload.auth

      return {...state, links, isAuthLoading: false}
    }
    case 'ME_REQUESTED': {
      return {...state, isMeLoading: true}
    }
    case 'ME_RECEIVED': {
      const {me} = action.payload

      return {...state, me, isMeLoading: false}
    }
    case 'LOGOUT': {
      return {}
    }
  }

  return state
}

export default auth
