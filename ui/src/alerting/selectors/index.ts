import {AppState, ResourceType} from 'src/types'
import {sortBy, get} from 'lodash'

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
