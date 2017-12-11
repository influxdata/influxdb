const getInitialState = () => ({
  links: null,
  me: null,
  isMeLoading: false,
  isAuthLoading: false,
  logoutLink: null,
})

import {getMeRole} from 'shared/reducers/helpers/auth'

export const initialState = getInitialState()

const meGetCompleted = (state, {me}, isUsingAuth) => {
  let newMe = me

  if (isUsingAuth) {
    newMe = {
      ...newMe,
      role: getMeRole(me),
      currentOrganization: me.currentOrganization,
    }
  }

  return {
    ...state,
    me: {...newMe},
    isMeLoading: false,
  }
}

const authReceived = (state, {auth: {links}}) => ({
  ...state,
  links,
  isAuthLoading: false,
})

const logoutLinkReceived = (state, {logoutLink}, isUsingAuth) => ({
  ...state,
  logoutLink,
  isUsingAuth,
})

const authReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'AUTH_EXPIRED': {
      const {auth: {links}} = action.payload
      return {...initialState, links}
    }
    case 'AUTH_REQUESTED': {
      return {...state, isAuthLoading: true}
    }
    case 'ME_GET_REQUESTED': {
      return {...state, isMeLoading: true}
    }
    case 'ME_GET_COMPLETED': {
      const {logoutLink} = action.payload
      const isUsingAuth = !!logoutLink

      let newState = meGetCompleted(state, action.payload, isUsingAuth)
      newState = authReceived(newState, action.payload)
      newState = logoutLinkReceived(newState, action.payload, isUsingAuth)

      return newState
    }
  }

  return state
}

export default authReducer
