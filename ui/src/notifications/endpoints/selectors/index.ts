import {AppState} from 'src/types'

export const getEndpointIDs = (state: AppState): {[x: string]: boolean} => {
  return state.resources.endpoints.allIDs.reduce(
    (acc, id) => ({...acc, [id]: true}),
    {}
  )
}
