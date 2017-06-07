import {default as errorsReducer, initialState} from 'shared/reducers/errors'

import {errorThrown} from 'shared/actions/errors'

import {HTTP_FORBIDDEN} from 'shared/constants'

const errorForbidden = {
  data: '',
  status: 403,
  statusText: 'Forbidden',
  headers: {
    date: 'Mon, 17 Apr 2017 18:35:34 GMT',
    'content-length': '0',
    'x-chronograf-version': '1.2.0-beta8-71-gd875ea4a',
    'content-type': 'text/plain; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    headers: {
      Accept: 'application/json, text/plain, */*',
      'Content-Type': 'application/json;charset=utf-8',
    },
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    method: 'GET',
    url: '/chronograf/v1/me',
    data: '{}',
    params: {},
  },
  request: {},
  auth: {
    links: [
      {
        name: 'github',
        label: 'Github',
        login: '/oauth/github/login',
        logout: '/oauth/github/logout',
        callback: '/oauth/github/callback',
      },
    ],
  },
}

describe('Shared.Reducers.errorsReducer', () => {
  it('should handle ERROR_THROWN', () => {
    const reducedState = errorsReducer(
      initialState,
      errorThrown(errorForbidden)
    )

    expect(reducedState.error.status).to.equal(HTTP_FORBIDDEN)
  })
})
