import {
  setRequestHandler,
  setResponseHandler,
  postSignout,
} from './generatedRoutes'
import {getAPIBasepath} from 'src/utils/basepath'

setRequestHandler((url, query, init) => {
  return {
    url: `${getAPIBasepath()}${url}`,
    query,
    init,
  }
})

setResponseHandler((status, headers, data) => {
  // if the user is inactive log them out
  // influxdb/http/authentication_middleware.go
  if (status === 403 && data.message === 'User is inactive') {
    postSignout({})
    window.location.href = '/signin'
  }

  return {status, headers, data}
})

export * from './generatedRoutes'
