import {RemoteDataState, ResourceType} from 'src/types'
import {getResourcesStatus} from 'src/shared/selectors/getResourcesStatus'

const getResourcesStatusUntyped: any = getResourcesStatus

describe('getResourcesStatus', () => {
  it('should return RemoteDataState.Done when all the resources are done loading', () => {
    const state = {
      labels: {
        status: RemoteDataState.Done,
      },
      buckets: {
        status: RemoteDataState.Done,
      },
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(
      RemoteDataState.Done
    )
  })
  it('should return RemoteDataState.Loading when when any of the resources are loading', () => {
    const state = {
      labels: {
        status: RemoteDataState.Done,
      },
      buckets: {
        status: RemoteDataState.Loading,
      },
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(
      RemoteDataState.Loading
    )
  })
  it('should return RemoteDataState.Error when when any of the resources error', () => {
    const state = {
      labels: {
        status: RemoteDataState.Error,
      },
      buckets: {
        status: RemoteDataState.Loading,
      },
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(
      RemoteDataState.Error
    )
  })
  it('should throw an error when when any of the resources have no status', () => {
    const state1 = {
      labels: {},
      buckets: {},
    }

    const state2 = {
      labels: {status: RemoteDataState.Done},
    }

    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(() => getResourcesStatusUntyped(state1, resources)).toThrowError()
    expect(() => getResourcesStatusUntyped(state2, resources)).toThrowError()
  })
})
