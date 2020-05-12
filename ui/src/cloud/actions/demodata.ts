// API
import {
  getDemoDataBuckets as getDemoDataBucketsAJAX,
  getDemoDataBucketMembership as getDemoDataBucketMembershipAJAX,
  deleteDemoDataBucketMembership as deleteDemoDataBucketMembershipAJAX,
  getNormalizedDemoDataBucket,
} from 'src/cloud/apis/demodata'
import {createDashboardFromTemplate} from 'src/templates/api'
import {getBucket} from 'src/client'

// Actions
import {addBucket, removeBucket} from 'src/buckets/actions/creators'
import {notify} from 'src/shared/actions/notifications'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Constants
import {DemoDataTemplates} from 'src/cloud/constants'
import {
  demoDataAddBucketFailed,
  demoDataDeleteBucketFailed,
  demoDataSucceeded,
} from 'src/shared/copy/notifications'

// Types
import {Bucket, RemoteDataState, GetState, DemoBucket} from 'src/types'
import {reportError} from 'src/shared/utils/errors'
import {getErrorMessage} from 'src/utils/api'

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

    const normalizedBucket = await getNormalizedDemoDataBucket(bucketID)

    dispatch(addBucket(normalizedBucket))
  } catch (error) {
    const message = `Failed to add demodata bucket ${bucketName}: ${getErrorMessage(
      error
    )}`

    dispatch(notify(demoDataAddBucketFailed(message)))

    reportError(error, {
      name: 'getDemoDataBucketMembership function',
    })

    return
  }

  try {
    const template = await DemoDataTemplates[bucketName]

    if (!template) {
      throw new Error(`dashboard template was not found`)
    }

    const createdDashboard = await createDashboardFromTemplate(template, orgID)

    const url = `/orgs/${orgID}/dashboards/${createdDashboard.id}`

    dispatch(notify(demoDataSucceeded(bucketName, url)))
  } catch (error) {
    const message = `Could not create dashboard for demodata bucket ${bucketName}: ${getErrorMessage(
      error
    )}`

    dispatch(notify(demoDataAddBucketFailed(message)))

    reportError(error, {
      name: 'getDemoDataBucketMembership function',
    })
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
  }
}
