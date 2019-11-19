// Types
import {AppState, RemoteDataState} from 'src/types'
import {ResourceType} from 'src/shared/components/GetResources'
import {TestState} from 'src/shared/selectors/getResourcesStatus.test'

export const getResourcesStatus = (
  state: AppState | TestState,
  resources: Array<ResourceType>
): RemoteDataState => {
  const done = resources.every(resource => {
    return state[resource].status === 'Done'
  })

  const loading = resources.some(resource => {
    return state[resource].status === 'Loading'
  })

  const error = resources.some(resource => {
    return state[resource].status === 'Error'
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
