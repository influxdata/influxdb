// Types
import {AppState, ResourceType} from 'src/types'

export const getAll = (state: AppState, resource: ResourceType) => {
  const {resources} = state
  const allIDs: string[] = resources[resource].allIDs
  const byID = resources[resource].byID
  return allIDs.map(id => byID[id])
}
