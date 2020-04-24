// API
import {
  getDemoDataBuckets as getDemoDataBucketsAJAX,
  getDemoDataBucketMembership as getDemoDataBucketMembershipAJAX,
  deleteDemoDataBucketMembership as deleteDemoDataBucketMembershipAJAX,
} from 'src/cloud/apis/demodata'
import {createDashboardFromTemplate} from 'src/templates/api'
import {getBucket} from 'src/client'

// Actions
import {addBucket, removeBucket} from 'src/buckets/actions/creators'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {normalize} from 'normalizr'

// Constants
import {DemoDataTemplates} from 'src/cloud/constants'

// Types
import {
  Bucket,
  RemoteDataState,
  GetState,
  DemoBucket,
  BucketEntities,
} from 'src/types'
import {bucketSchema} from 'src/schemas'

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

export const getDemoDataBucketMembership = (bucket: DemoBucket) => async (
  dispatch,
  getState: GetState
) => {
  const state = getState()
  const {
    me: {id: userID},
  } = state
  const {id: orgID} = getOrg(state)

  try {
    await getDemoDataBucketMembershipAJAX(bucket.id, userID)

    const template = await DemoDataTemplates[bucket.name]
    if (template) {
      await createDashboardFromTemplate(template, orgID)
    } else {
      throw new Error(
        `Could not find template for demodata bucket ${bucket.name}`
      )
    }

    const resp = await getBucket({bucketID: bucket.id})

    if (resp.status !== 200) {
      throw new Error('Request for demo data bucket membership did not succeed')
    }

    const newBucket = {
      ...resp.data,
      type: 'demodata' as 'demodata',
      labels: [],
    } as DemoBucket

    const normalizedBucket = normalize<Bucket, BucketEntities, string>(
      newBucket,
      bucketSchema
    )

    dispatch(addBucket(normalizedBucket))

    // TODO: notify success and error appropriately
  } catch (error) {
    console.error(error)
  }
}

export const deleteDemoDataBucketMembership = (bucket: DemoBucket) => async (
  dispatch,
  getState: GetState
) => {
  const {
    me: {id: userID},
  } = getState()

  try {
    await deleteDemoDataBucketMembershipAJAX(bucket.id, userID)

    const resp = await getBucket({bucketID: bucket.id})

    if (resp.status === 200) {
      throw new Error('Request to remove demo data bucket did not succeed')
    }

    dispatch(removeBucket(bucket.id))

    // TODO: notify for success and error appropriately
  } catch (error) {
    console.error(error)
  }
}
