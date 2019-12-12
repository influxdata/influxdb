// Types
import {AppState, RemoteDataState} from 'src/types'
import {ResourceType} from 'src/shared/components/GetResources'

export const getResourcesStatus = (
  state: AppState,
  resources: Array<ResourceType>
): RemoteDataState => {
  const statuses = resources.map(resource => {
    if (!state[resource] || !state[resource].status) {
      throw new Error(
        `Loading status for resource ${resource} is undefined in getResourcesStatus`
      )
    }
    return state[resource].status
  })

  let status = RemoteDataState.NotStarted

  if (statuses.every(s => s === RemoteDataState.Done)) {
    status = RemoteDataState.Done
  } else if (statuses.includes(RemoteDataState.Error)) {
    status = RemoteDataState.Error
  } else if (statuses.includes(RemoteDataState.Loading)) {
    status = RemoteDataState.Loading
  }

  return status
}
