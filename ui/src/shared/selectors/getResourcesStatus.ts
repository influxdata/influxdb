// Types
import {AppState, RemoteDataState, ResourceType} from 'src/types'

export const getResourcesStatus = (
  state: AppState,
  resources: Array<ResourceType>
): RemoteDataState => {
  const statuses = resources.map(resource => {
    switch (resource) {
      // Normalized resource status
      case ResourceType.Members: {
        return state.resources[resource].status
      }

      default:
        // Get status for resources that have not yet been normalized
        return getStatus(state, resource)
    }
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

const getStatus = (state: AppState, resource: ResourceType) => {
  if (!state[resource] || !state[resource].status) {
    throw new Error(
      `Loading status for resource "${resource}" is undefined in getResourcesStatus`
    )
  }

  return state[resource].status
}
