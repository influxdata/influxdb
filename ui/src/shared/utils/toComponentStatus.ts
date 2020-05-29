import {ComponentStatus} from 'src/clockface'
import {RemoteDataState} from 'src/types'

export const toComponentStatus = (status: RemoteDataState): ComponentStatus => {
  if (status === RemoteDataState.NotStarted) {
    return ComponentStatus.Disabled
  }

  if (status === RemoteDataState.Loading) {
    return ComponentStatus.Loading
  }

  if (status === RemoteDataState.Error) {
    return ComponentStatus.Error
  }

  return ComponentStatus.Default
}
