import {setRequestHandler, setResponseHandler, postSignout} from './generatedRoutes'
import {getAPIBasepath} from 'src/utils/basepath'

setRequestHandler((url, query, init) => {
  return {
    url: `${getAPIBasepath()}${url}`,
    query,
    init,
  }
})

setResponseHandler((status, headers, data) => {
  if (status === 403) {
    postSignout({})
    window.location.href = '/signin'
  }

  return {status, headers, data}
})

export * from './generatedRoutes'
