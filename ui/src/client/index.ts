import {setRequestHandler, setResponseHandler, postSignout} from './generatedRoutes'
import {getBasepath} from 'src/utils/basepath'

setRequestHandler((url, query, init) => {
  return {
    url: getBasepath() + url,
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
