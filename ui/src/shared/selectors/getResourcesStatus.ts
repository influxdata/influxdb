// Types
import {AppState, RemoteDataState} from 'src/types'
import {Props} from 'src/shared/components/GetResources'

export const getResourcesStatus = (
  state: AppState,
  {resources}: Props
): RemoteDataState => {
  const done = resources.every(resource => {
    if (state[resource] && state[resource].status) {
      return state[resource].status === 'Done'
    }
    return false
  })

  const loading = resources.some(resource => {
    if (state[resource] && state[resource].status) {
      return state[resource].status === 'Loading'
    }
  })

  const error = resources.some(resource => {
    if (state[resource] && state[resource].status) {
      return state[resource].status === 'Error'
    }
    if (state[resource] && state[resource].status === false) {
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
