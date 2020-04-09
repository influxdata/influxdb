// API
import {
  getDemoDataBuckets as getDemoDataBucketsAJAX,
  getDemoDataBucketMembership as getDemoDataBucketMembershipAJAX,
  deleteDemoDataBucketMembership as deleteDemoDataBucketMembershipAJAX,
} from 'src/cloud/apis/demodata'
import {createDashboardFromTemplate} from 'src/templates/api'
import * as api from 'src/client'

// Actions
import {getBuckets} from 'src/buckets/actions/thunks'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Constants
import {DemoDataTemplates, DemoDataDashboards} from 'src/cloud/constants'

// Types
import {Bucket, RemoteDataState, GetState, DemoBucket} from 'src/types'
import {getDashboards} from 'src/dashboards/actions/thunks'

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
    dispatch(getBuckets())

    const template = DemoDataTemplates[bucket.name]
    if (template) {
      await createDashboardFromTemplate(template, orgID)
    } else {
      throw new Error(
        `Could not find template for demodata bucket ${bucket.name}`
      )
    }

    // TODO: notify success and error appropriately
  } catch (error) {
    console.error(error)
  }
}

export const deleteDemoDataBucketMembership = (bucket: DemoBucket) => async (
  dispatch,
  getState: GetState
) => {
  const state = getState()
  const {
    me: {id: userID},
  } = state
  const {id: orgID} = getOrg(state)

  try {
    await deleteDemoDataBucketMembershipAJAX(bucket.id, userID)
    dispatch(getBuckets())

    const dashboardsResp = await api.getDashboards({query: {orgID}})

    if (dashboardsResp.status !== 200) {
      throw new Error(dashboardsResp.data.message)
    }

    const demoDashboardName = DemoDataDashboards[bucket.name]
    if (!demoDashboardName) {
      throw new Error(
        `Could not find dashboard name for demo data bucket ${bucket.name}`
      )
    }

    const ddDashboard = dashboardsResp.data.dashboards.find(d => {
      d.name === demoDashboardName
    })

    if (ddDashboard) {
      const deleteResp = await api.deleteDashboard({
        dashboardID: ddDashboard.id,
      })
      if (deleteResp.status !== 204) {
        throw new Error(deleteResp.data.message)
      }
      dispatch(getDashboards)
    }

    // TODO: notify for success and error appropriately
  } catch (error) {
    console.error(error)
  }
}
