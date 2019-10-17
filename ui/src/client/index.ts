import {setResponseHandler, postSignout} from './generatedRoutes'

setResponseHandler((status, headers, data) => {
  if (status === 403) {
    postSignout({})
    window.location.href = '/signin'
  }

  return {status, headers, data}
})

export * from './generatedRoutes'
