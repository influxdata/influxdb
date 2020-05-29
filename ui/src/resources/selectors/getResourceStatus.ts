// Libraries
import {get} from 'lodash'

// Types
import {AppState, RemoteDataState} from 'src/types'
import {Resource} from 'src/resources/components/GetResource'

export const getResourceStatus = (
  state: AppState,
  resources: Resource[]
): RemoteDataState => {
  const statuses = resources.map(resource => {
    return getStatus(state, resource)
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

const getStatus = ({resources}: AppState, {type, id}: Resource) => {
  return get(resources, [type, 'byID', id, 'status'], RemoteDataState.Loading)
}
