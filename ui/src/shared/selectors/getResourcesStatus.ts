// Types
import {AppState, RemoteDataState} from 'src/types'
import {Props} from 'src/shared/components/GetResources'

export const getResourcesStatus = (
  state: AppState,
  {resources}: Props
): RemoteDataState => {
  const resourceExists = (state, resource): boolean =>
    state[resource] && state[resource].status

  const done = resources.every(resource => {
    if (resourceExists(state, resource)) {
      return state[resource].status === 'Done'
    }
    return false
  })

  const loading = resources.some(resource => {
    if (resourceExists(state, resource)) {
      return state[resource].status === 'Loading'
    }
  })

  const error = resources.some(resource => {
    if (resourceExists(state, resource)) {
      return state[resource].status === 'Error'
    }
    if (resourceExists(state, resource) === false) {
      // if the resource doesn't exist in the state return an error
      return true
    }
  })

  let status = RemoteDataState.NotStarted

  if (done) {
    status = RemoteDataState.Done
  }

  if (loading) {
    status = RemoteDataState.Loading
  }

  if (error) {
    status = RemoteDataState.Error
  }

  return status
}
