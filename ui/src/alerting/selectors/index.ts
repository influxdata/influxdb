import {AppState, Check, ResourceType} from 'src/types'

export const getCheck = (state: AppState, id: string): Check => {
  const checksList = state.checks.list
  return checksList.find(c => c.id === id)
}

export const getResourceIDs = (
  state: AppState,
  resource: ResourceType
): {[x: string]: boolean} => {
  return state[resource].list.reduce(
    (acc, endpoint) => ({...acc, [endpoint.id]: true}),
    {}
  )
}
