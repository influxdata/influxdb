// Libraries
import {get} from 'lodash'

// Types
import {AppState, ResourceType, RemoteDataState} from 'src/types'

export const getStatus = ({resources}: AppState, resource): RemoteDataState => {
  return resources[resource].status
}

export const getAll = <R>({resources}: AppState, resource): R[] => {
  const allIDs: string[] = resources[resource].allIDs
  const byID: {[uuid: string]: R} = resources[resource].byID
  return allIDs.map(id => byID[id])
}

export const getByID = <R>(
  {resources}: AppState,
  type: ResourceType,
  id: string
): R => {
  const byID = get(resources, `${type}.byID`)

  if (!byID) {
    throw new Error(`"${type}" resource has yet not been set`)
  }

  const resource = get(byID, `${id}`)

  if (!resource) {
    throw new Error(`Could not find resource of type "${type}" with id "${id}"`)
  }

  return resource
}
