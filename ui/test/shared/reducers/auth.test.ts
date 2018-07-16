import {default as authReducer, initialState} from 'src/shared/reducers/auth'

import {
  authExpired,
  authRequested,
  meGetRequested,
  meGetCompleted,
} from 'src/shared/actions/auth'

const defaultAuth = {
  links: [
    {
      name: 'github',
      label: 'Github',
      login: '/oauth/github/login',
      logout: '/oauth/github/logout',
      callback: '/oauth/github/callback',
    },
  ],
  me: null,
  isMeLoading: false,
  isAuthLoading: false,
  logoutLink: null,
  isUsingAuth: false,
}

const defaultMe = {
  name: 'wishful_modal@overlay.technology',
  links: {
    self: '/chronograf/v1/users/wishful_modal@overlay.technology',
  },
}

describe('Shared.Reducers.authReducer', () => {
  it('should handle AUTH_EXPIRED', () => {
    const reducedState = authReducer(initialState, authExpired(defaultAuth))

    expect(reducedState.links[0]).toEqual(defaultAuth.links[0])
    expect(reducedState.me).toBe(null)
    expect(reducedState.isMeLoading).toBe(false)
    expect(reducedState.isAuthLoading).toBe(false)
  })

  it('should handle AUTH_REQUESTED', () => {
    const reducedState = authReducer(initialState, authRequested())

    expect(reducedState.isAuthLoading).toBe(true)
  })

  it('should handle ME_GET_REQUESTED', () => {
    const reducedState = authReducer(initialState, meGetRequested())

    expect(reducedState.isMeLoading).toBe(true)
  })

  it('should handle ME_GET_COMPLETED with auth', () => {
    const loadingState = {
      ...initialState,
      isAuthLoading: true,
      isMeLoading: true,
    }

    const meWithAuth = {
      ...defaultMe,
      roles: [{name: 'member', organization: '1'}],
      role: 'member',
      currentOrganization: {name: 'bob', id: '1'},
    }

    const reducedState = authReducer(
      loadingState,
      meGetCompleted({
        me: meWithAuth,
        auth: defaultAuth,
        logoutLink: '/oauth/logout',
      })
    )

    expect(reducedState.me).toEqual(meWithAuth)
    expect(reducedState.links[0]).toEqual(defaultAuth.links[0])
    expect(reducedState.isAuthLoading).toBe(false)
    expect(reducedState.isMeLoading).toBe(false)
  })

  it('should handle ME_GET_COMPLETED without auth', () => {
    const loadingState = {
      ...initialState,
      isAuthLoading: true,
      isMeLoading: true,
    }
    const reducedState = authReducer(
      loadingState,
      meGetCompleted({
        me: defaultMe,
        auth: defaultAuth,
        logoutLink: 'foo',
      })
    )

    expect(reducedState.me).toEqual(defaultMe)
    expect(reducedState.links[0]).toEqual(defaultAuth.links[0])
    expect(reducedState.isAuthLoading).toBe(false)
    expect(reducedState.isMeLoading).toBe(false)
  })
})
