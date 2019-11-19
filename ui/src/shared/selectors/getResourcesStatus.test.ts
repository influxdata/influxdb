import {RemoteDataState} from 'src/types'
import {ResourceType} from 'src/shared/components/GetResources'
import {getResourcesStatus} from 'src/shared/selectors/getResourcesStatus'

export interface TestState {
  buckets?: object
  labels?: object
}

describe('getResourcesStatus', () => {
  it('should return RemoteDataState.Done when all the resources are done loading', () => {
    const state: TestState = {
      labels: {
        status: RemoteDataState.Done,
      },
    }
    const resources = [ResourceType.Labels]
    expect(getResourcesStatus(state, resources)).toEqual(RemoteDataState.Done)
    state.buckets = {
      status: RemoteDataState.Done,
    }
    resources.push(ResourceType.Buckets)
    expect(getResourcesStatus(state, resources)).toEqual(RemoteDataState.Done)
  })
  it('should return RemoteDataState.Loading when when any of the resources are loading', () => {
    const state: TestState = {
      labels: {
        status: RemoteDataState.Done,
      },
      buckets: {
        status: RemoteDataState.Loading,
      },
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatus(state, resources)).toEqual(
      RemoteDataState.Loading
    )
  })
  it('should return RemoteDataState.Error when when any of the resources error', () => {
    const state: TestState = {
      labels: {
        status: RemoteDataState.Error,
      },
      buckets: {
        status: RemoteDataState.Loading,
      },
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatus(state, resources)).toEqual(RemoteDataState.Error)
  })
  it('should return RemoteDataState.NotStarted when when any of the resources have no status', () => {
    const state: TestState = {
      labels: {},
      buckets: {},
    }
    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(getResourcesStatus(state, resources)).toEqual(
      RemoteDataState.NotStarted
    )
  })
})
