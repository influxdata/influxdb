// Types
import {AppState, RemoteDataState} from 'src/types'
import {Props} from 'src/shared/components/GetResources'

export const getResourcesStatus = (
  state: AppState,
  {resources}: Props
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
