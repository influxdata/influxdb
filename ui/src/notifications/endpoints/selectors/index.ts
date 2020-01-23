import {AppState} from 'src/types'

export const getEndpointIDs = (state: AppState): {[x: string]: boolean} => {
  return state.endpoints.list.reduce(
    (acc, endpoint) => ({...acc, [endpoint.id]: true}),
    {}
  )
}
