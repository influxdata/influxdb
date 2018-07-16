import {Me, AuthLink} from 'src/types/auth'
import {getMeRole} from 'src/shared/reducers/helpers/auth'
import {getDeep} from 'src/utils/wrappers'

import {ActionTypes} from 'src/shared/actions/auth'

interface State {
  links: AuthLink[] | null
  me: Me | null
  isMeLoading: boolean
  isAuthLoading: boolean
  logoutLink: string | null
  isUsingAuth: boolean
}

interface Auth {
  auth: {
    links: AuthLink[]
  }
}

const getInitialState = (): State => ({
  links: null,
  me: null,
  isMeLoading: false,
  isAuthLoading: false,
  logoutLink: null,
  isUsingAuth: false,
})

export const initialState = getInitialState()

const meGetCompleted = (
  state: State,
  {me}: {me: Me},
  isUsingAuth: boolean
): State => {
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

const authReceived = (state: State, auth: Auth): State => {
  const links = getDeep<AuthLink[]>(auth, 'auth.links', [])
  return {
    ...state,
    links,
    isAuthLoading: false,
  }
}

const logoutLinkReceived = (
  state: State,
  {logoutLink}: {logoutLink: string},
  isUsingAuth: boolean
): State => ({
  ...state,
  logoutLink,
  isUsingAuth,
})

const authReducer = (state: State = initialState, action): State => {
  switch (action.type) {
    case ActionTypes.AuthExpired: {
      const {
        auth: {links},
      } = action.payload
      return {...initialState, links}
    }
    case ActionTypes.AuthRequested: {
      return {...state, isAuthLoading: true}
    }
    case ActionTypes.MeGetRequested: {
      return {...state, isMeLoading: true}
    }
    case ActionTypes.MeGetCompleted: {
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
