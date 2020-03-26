// API
import {
  getDemoDataBuckets as getDemoDataBucketsAJAX,
  getDemoDataBucketMembership as getDemoDataBucketMembershipAJAX,
  deleteDemoDataBucketMembership as deleteDemoDataBucketMembershipAJAX,
} from 'src/cloud/apis/demodata'

// Types
import {Bucket, RemoteDataState, GetState} from 'src/types'
import {getBuckets} from 'src/buckets/actions/thunks'

export type Actions =
  | ReturnType<typeof setDemoDataStatus>
  | ReturnType<typeof setDemoDataBuckets>

export const setDemoDataStatus = (status: RemoteDataState) => ({
  type: 'SET_DEMODATA_STATUS' as 'SET_DEMODATA_STATUS',
  payload: {status},
})

export const setDemoDataBuckets = (buckets: Bucket[]) => ({
  type: 'SET_DEMODATA_BUCKETS' as 'SET_DEMODATA_BUCKETS',
  payload: {buckets},
})

export const getDemoDataBuckets = () => async (
  dispatch,
  getState: GetState
) => {
  const {
    cloud: {
      demoData: {status},
    },
  } = getState()
  if (status === RemoteDataState.NotStarted) {
    dispatch(setDemoDataStatus(RemoteDataState.Loading))
  }
  try {
    const buckets = await getDemoDataBucketsAJAX()

    dispatch(setDemoDataStatus(RemoteDataState.Done))
    dispatch(setDemoDataBuckets(buckets))
  } catch (error) {
    console.error(error)
    dispatch(setDemoDataStatus(RemoteDataState.Error))
  }
}

export const getDemoDataBucketMembership = (bucketID: string) => async (
  dispatch,
  getState: GetState
) => {
  const {
    me: {id: userID},
  } = getState()

  try {
    await getDemoDataBucketMembershipAJAX(bucketID, userID)

    dispatch(getBuckets())
    // TODO: check for success and error appropriately
    // TODO: instantiate dashboard template
  } catch (error) {
    console.error(error)
  }
}

export const deleteDemoDataBucketMembership = (bucketID: string) => async (
  dispatch,
  getState: GetState
) => {
  const {
    me: {id: userID},
  } = getState()

  try {
    await deleteDemoDataBucketMembershipAJAX(bucketID, userID)
    dispatch(getBuckets())

    // TODO: check for success and error appropriately
    // TODO: delete associated dashboard
  } catch (error) {
    console.error(error)
  }
}
