// Libraries
import {get} from 'lodash'

// Types
import {AppState, ResourceType, RemoteDataState, Label} from 'src/types'

export const getStatus = (
  {resources}: AppState,
  resource: ResourceType
): RemoteDataState => {
  return resources[resource].status
}

export const getAll = <R>(
  {resources}: AppState,
  resource: ResourceType
): R[] => {
  const allIDs: string[] = resources[resource].allIDs
  const byID: {[uuid: string]: R} = resources[resource].byID
  return allIDs.map(id => byID[id])
}

export const getToken = (state: AppState): string =>
  get(state, 'dataLoading.dataLoaders.token', '') || ''

export const getByID = <R>(
  {resources}: AppState,
  type: ResourceType,
  id: string
): R => {
  const byID = get(resources, `${type}.byID`)

  if (!byID) {
    throw new Error(`"${type}" resource has yet not been set`)
  }

  const resource = get(byID, `${id}`, null)

  return resource
}

export const getLabels = (state: AppState, labelIDs: string[]): Label[] => {
  return labelIDs
    .map(labelID => getByID<Label>(state, ResourceType.Labels, labelID))
    .filter(label => !!label)
}
