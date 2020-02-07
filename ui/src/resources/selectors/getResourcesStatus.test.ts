import {RemoteDataState, ResourceType} from 'src/types'
import {getResourcesStatus} from 'src/resources/selectors/getResourcesStatus'

// Types
const getResourcesStatusUntyped: any = getResourcesStatus
const {NotStarted, Done, Loading} = RemoteDataState
const {Labels, Buckets} = ResourceType

type StatusTuple = [RemoteDataState, RemoteDataState]

const genState = (statuses: StatusTuple = [NotStarted, NotStarted]) => ({
  resources: {
    buckets: {
      byID: {},
      allIDs: [],
      status: statuses[1],
    },
    labels: {
      byID: {},
      allIDs: [],
      status: statuses[0],
    },
  },
})

describe('getResourcesStatus', () => {
  it('should return RemoteDataState.Done when all the resources are done loading', () => {
    const state = genState([Done, Done])
    const resources = [Labels, Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(Done)
  })

  it('should return RemoteDataState.Loading when when any of the resources are loading', () => {
    const state = genState([Done, Loading])
    const resources = [Labels, Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(Loading)
  })

  it('should return RemoteDataState.Error when when any of the resources error', () => {
    const state = genState([RemoteDataState.Error, Loading])
    const resources = [Labels, Buckets]
    expect(getResourcesStatusUntyped(state, resources)).toEqual(
      RemoteDataState.Error
    )
  })

  it('should throw an error when when any of the resources have no status', () => {
    const state1 = {
      labels: {},
      resources: {},
    }

    const state2 = {
      labels: {status: RemoteDataState.Done},
    }

    const resources = [ResourceType.Labels, ResourceType.Buckets]
    expect(() => getResourcesStatusUntyped(state1, resources)).toThrowError()
    expect(() => getResourcesStatusUntyped(state2, resources)).toThrowError()
  })
})
