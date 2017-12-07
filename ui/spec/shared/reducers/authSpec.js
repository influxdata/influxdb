import {default as authReducer, initialState} from 'shared/reducers/auth'

import {
  authExpired,
  authRequested,
  authReceived,
  meGetRequested,
  meGetCompleted,
} from 'shared/actions/auth'

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

    expect(reducedState.links[0]).to.deep.equal(defaultAuth.links[0])
    expect(reducedState.me).to.equal(null)
    expect(reducedState.isMeLoading).to.equal(false)
    expect(reducedState.isAuthLoading).to.equal(false)
  })

  it('should handle AUTH_REQUESTED', () => {
    const reducedState = authReducer(initialState, authRequested())

    expect(reducedState.isAuthLoading).to.equal(true)
  })

  it('should handle ME_GET_REQUESTED', () => {
    const reducedState = authReducer(initialState, meGetRequested())

    expect(reducedState.isMeLoading).to.equal(true)
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

    expect(reducedState.me).to.deep.equal(meWithAuth)
    expect(reducedState.links[0]).to.deep.equal(defaultAuth.links[0])
    expect(reducedState.isAuthLoading).to.equal(false)
    expect(reducedState.isMeLoading).to.equal(false)
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
      })
    )

    expect(reducedState.me).to.deep.equal(defaultMe)
    expect(reducedState.links[0]).to.deep.equal(defaultAuth.links[0])
    expect(reducedState.isAuthLoading).to.equal(false)
    expect(reducedState.isMeLoading).to.equal(false)
  })
})
