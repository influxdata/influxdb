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
import {notify} from 'src/shared/actions/notifications'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {normalize} from 'normalizr'
import {getAll} from 'src/resources/selectors'

// Constants
import {DemoDataTemplates, DemoDataDashboards} from 'src/cloud/constants'
import {
  demoDataAddBucketFailed,
  demoDataDeleteBucketFailed,
  demoDataSucceeded,
} from 'src/shared/copy/notifications'

// Types
import {
  Bucket,
  RemoteDataState,
  GetState,
  DemoBucket,
  BucketEntities,
  ResourceType,
  Dashboard,
} from 'src/types'
import {bucketSchema} from 'src/schemas'
import {reportError} from 'src/shared/utils/errors'

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

    dispatch(setDemoDataBuckets(buckets))
  } catch (error) {
    console.error(error)

    reportError(error, {
      name: 'getDemoDataBuckets function',
    })

    dispatch(setDemoDataStatus(RemoteDataState.Error))
  }
}

export const getDemoDataBucketMembership = ({
  name: bucketName,
  id: bucketID,
}) => async (dispatch, getState: GetState) => {
  const state = getState()

  const {
    me: {id: userID},
  } = state

  const {id: orgID} = getOrg(state)

  try {
    await getDemoDataBucketMembershipAJAX(bucketID, userID)

    const resp = await getBucket({bucketID})

    if (resp.status !== 200) {
      throw new Error(
        `Request for demo data bucket membership did not succeed: ${
          resp.data.message
        }`
      )
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

    const template = await DemoDataTemplates[bucketName]
    if (!template) {
      throw new Error(
        `Could not find dashboard template for demodata bucket ${bucketName}`
      )
    }

    await createDashboardFromTemplate(template, orgID)
    const updatedState = getState()

    const allDashboards = getAll<Dashboard>(
      updatedState,
      ResourceType.Dashboards
    )

    const createdDashboard = allDashboards.find(
      d => d.name === DemoDataDashboards[bucketName]
    )

    const url = `/orgs/${orgID}/dashboards/${createdDashboard.id}`

    dispatch(notify(demoDataSucceeded(bucketName, url)))
  } catch (error) {
    dispatch(notify(demoDataAddBucketFailed(error)))

    reportError(error, {
      name: 'getDemoDataBucketMembership function',
    })

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
  } catch (error) {
    dispatch(notify(demoDataDeleteBucketFailed(bucket.name, error)))

    reportError(error, {
      name: 'deleteDemoDataBucketMembership function',
    })

    console.error(error)
  }
}
