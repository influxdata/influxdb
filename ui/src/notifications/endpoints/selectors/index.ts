import {AppState, NotificationEndpoint} from 'src/types'

export const getEndpointIDs = (state: AppState): {[x: string]: boolean} => {
  return state.resources.endpoints.allIDs.reduce(
    (acc, id) => ({...acc, [id]: true}),
    {}
  )
}

export const sortEndpointsByName = (
  endpoints: NotificationEndpoint[]
): NotificationEndpoint[] =>
  endpoints.sort((a, b) =>
    a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1
  )
