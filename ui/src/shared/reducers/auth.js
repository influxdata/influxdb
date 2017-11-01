const getInitialState = () => ({
  links: null,
  me: null,
  isMeLoading: false,
  isAuthLoading: false,
  logoutLink: null,
})

export const initialState = getInitialState()

const authReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'AUTH_EXPIRED': {
      const {auth: {links}} = action.payload
      return {...initialState, links}
    }
    case 'AUTH_REQUESTED': {
      return {...state, isAuthLoading: true}
    }
    case 'AUTH_RECEIVED': {
      const {auth: {links}} = action.payload
      return {...state, links, isAuthLoading: false}
    }
    case 'ME_REQUESTED': {
      return {...state, isMeLoading: true}
    }
    case 'ME_RECEIVED': {
      const {me} = action.payload
      return {...state, me, isMeLoading: false}
    }
    case 'LOGOUT_LINK_RECEIVED': {
      const {logoutLink} = action.payload
      return {...state, logoutLink, isUsingAuth: !!logoutLink}
    }
  }

  return state
}

export default authReducer
