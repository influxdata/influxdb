import {default as authReducer, initialState} from 'shared/reducers/auth'

import {
  authExpired,
  authRequested,
  authReceived,
  meRequested,
  meReceived,
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
  password: '',
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
})
