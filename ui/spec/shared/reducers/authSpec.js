import {default as authReducer, initialState} from 'shared/reducers/auth'

import {
  authExpired,
  authRequested,
  authReceived,
  meRequested,
  meReceivedNotUsingAuth,
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

  it('should handle AUTH_RECEIVED', () => {
    const loadingState = Object.assign({}, initialState, {isAuthLoading: true})
    const reducedState = authReducer(loadingState, authReceived(defaultAuth))

    expect(reducedState.links[0]).to.deep.equal(defaultAuth.links[0])
    expect(reducedState.isAuthLoading).to.equal(false)
  })

  it('should handle ME_REQUESTED', () => {
    const reducedState = authReducer(initialState, meRequested())

    expect(reducedState.isMeLoading).to.equal(true)
  })

  it('should handle ME_RECEIVED__NON_AUTH', () => {
    const loadingState = {...initialState, isMeLoading: true}
    const reducedState = authReducer(
      loadingState,
      meReceivedNotUsingAuth(defaultMe)
    )

    expect(reducedState.me).to.deep.equal(defaultMe)
    expect(reducedState.isAuthLoading).to.equal(false)
  })
})
