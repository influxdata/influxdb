import {AppState, Check, ResourceType} from 'src/types'
import {sortBy, get} from 'lodash'

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

interface HasName {
  name?: string
}

export const getResourceList = <T extends HasName>(
  state: AppState,
  resource: ResourceType
): T[] => {
  const resourceList: T[] = get(state, `${resource}.list`, [])
  return sortBy(resourceList, l => {
    return l.name.toLocaleLowerCase()
  })
}
