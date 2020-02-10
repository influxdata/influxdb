// API
import {
  getReadWriteCardinalityLimits as getReadWriteCardinalityLimitsAJAX,
  getLimits as getLimitsAJAX,
} from 'src/cloud/apis/limits'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {readLimitReached} from 'src/shared/copy/notifications'

// Types
import {
  Bucket,
  GetState,
  Limits,
  RemoteDataState,
  ResourceType,
} from 'src/types'

// Selectors
import {
  extractDashboardMax,
  extractBucketMax,
  extractTaskMax,
  extractChecksMax,
  extractRulesMax,
  extractEndpointsMax,
} from 'src/cloud/utils/limits'
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

export enum LimitStatus {
  OK = 'ok',
  EXCEEDED = 'exceeded',
}

export enum ActionTypes {
  SetLimits = 'SET_LIMITS',
  SetLimitsStatus = 'SET_LIMITS_STATUS',
  SetDashboardLimitStatus = 'SET_DASHBOARD_LIMIT_STATUS',
  SetBucketLimitStatus = 'SET_BUCKET_LIMIT_STATUS',
  SetTaskLimitStatus = 'SET_TASK_LIMIT_STATUS',
  SetChecksLimitStatus = 'SET_CHECKS_LIMIT_STATUS',
  SetRulesLimitStatus = 'SET_RULES_LIMIT_STATUS',
  SetEndpointsLimitStatus = 'SET_ENDPOINTS_LIMIT_STATUS',
  SetReadRateLimitStatus = 'SET_READ_RATE_LIMIT_STATUS',
  SetWriteRateLimitStatus = 'SET_WRITE_RATE_LIMIT_STATUS',
  SetCardinalityLimitStatus = 'SET_CARDINALITY_LIMIT_STATUS',
}

export type Actions =
  | SetLimits
  | SetLimitsStatus
  | SetDashboardLimitStatus
  | SetBucketLimitStatus
  | SetTaskLimitStatus
  | SetChecksLimitStatus
  | SetRulesLimitStatus
  | SetEndpointsLimitStatus
  | SetReadRateLimitStatus
  | SetWriteRateLimitStatus
  | SetCardinalityLimitStatus

export interface SetLimits {
  type: ActionTypes.SetLimits
  payload: {limits: Limits}
}

export const setLimits = (limits: Limits): SetLimits => {
  return {
    type: ActionTypes.SetLimits,
    payload: {limits},
  }
}

export interface SetDashboardLimitStatus {
  type: ActionTypes.SetDashboardLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setDashboardLimitStatus = (
  limitStatus: LimitStatus
): SetDashboardLimitStatus => {
  return {
    type: ActionTypes.SetDashboardLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetBucketLimitStatus {
  type: ActionTypes.SetBucketLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setBucketLimitStatus = (
  limitStatus: LimitStatus
): SetBucketLimitStatus => {
  return {
    type: ActionTypes.SetBucketLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetTaskLimitStatus {
  type: ActionTypes.SetTaskLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setTaskLimitStatus = (
  limitStatus: LimitStatus
): SetTaskLimitStatus => {
  return {
    type: ActionTypes.SetTaskLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetChecksLimitStatus {
  type: ActionTypes.SetChecksLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setChecksLimitStatus = (
  limitStatus: LimitStatus
): SetChecksLimitStatus => {
  return {
    type: ActionTypes.SetChecksLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetRulesLimitStatus {
  type: ActionTypes.SetRulesLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setRulesLimitStatus = (
  limitStatus: LimitStatus
): SetRulesLimitStatus => {
  return {
    type: ActionTypes.SetRulesLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetEndpointsLimitStatus {
  type: ActionTypes.SetEndpointsLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setEndpointsLimitStatus = (
  limitStatus: LimitStatus
): SetEndpointsLimitStatus => {
  return {
    type: ActionTypes.SetEndpointsLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetReadRateLimitStatus {
  type: ActionTypes.SetReadRateLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setReadRateLimitStatus = (
  limitStatus: LimitStatus
): SetReadRateLimitStatus => {
  return {
    type: ActionTypes.SetReadRateLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetWriteRateLimitStatus {
  type: ActionTypes.SetWriteRateLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setWriteRateLimitStatus = (
  limitStatus: LimitStatus
): SetWriteRateLimitStatus => {
  return {
    type: ActionTypes.SetWriteRateLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetCardinalityLimitStatus {
  type: ActionTypes.SetCardinalityLimitStatus
  payload: {limitStatus: LimitStatus}
}

export const setCardinalityLimitStatus = (
  limitStatus: LimitStatus
): SetCardinalityLimitStatus => {
  return {
    type: ActionTypes.SetCardinalityLimitStatus,
    payload: {limitStatus},
  }
}

export interface SetLimitsStatus {
  type: ActionTypes.SetLimitsStatus
  payload: {
    status: RemoteDataState
  }
}

export const setLimitsStatus = (status: RemoteDataState): SetLimitsStatus => {
  return {
    type: ActionTypes.SetLimitsStatus,
    payload: {status},
  }
}

export const getReadWriteCardinalityLimits = () => async (
  dispatch,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())

    const limits = await getReadWriteCardinalityLimitsAJAX(org.id)

    if (limits.read.status === LimitStatus.EXCEEDED) {
      dispatch(notify(readLimitReached()))
      dispatch(setReadRateLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setReadRateLimitStatus(LimitStatus.OK))
    }

    if (limits.write.status === LimitStatus.EXCEEDED) {
      dispatch(setWriteRateLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setWriteRateLimitStatus(LimitStatus.OK))
    }

    if (limits.cardinality.status === LimitStatus.EXCEEDED) {
      dispatch(setCardinalityLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setCardinalityLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const getAssetLimits = () => async (dispatch, getState: GetState) => {
  dispatch(setLimitsStatus(RemoteDataState.Loading))
  try {
    const org = getOrg(getState())

    const limits = await getLimitsAJAX(org.id)
    dispatch(setLimits(limits))
    dispatch(setLimitsStatus(RemoteDataState.Done))
  } catch (error) {
    console.error(error)
    dispatch(setLimitsStatus(RemoteDataState.Error))
  }
}

export const checkDashboardLimits = () => (dispatch, getState: GetState) => {
  try {
    const state = getState()
    const {
      cloud: {limits},
      resources,
    } = state

    const dashboardsMax = extractDashboardMax(limits)
    const dashboardsCount = resources.dashboards.allIDs.length

    if (dashboardsCount >= dashboardsMax) {
      dispatch(setDashboardLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setDashboardLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const checkBucketLimits = () => (dispatch, getState: GetState) => {
  try {
    const state = getState()
    const {
      cloud: {limits},
    } = state
    const allBuckets = getAll<Bucket>(state, ResourceType.Buckets)
    const bucketsMax = extractBucketMax(limits)
    const buckets = allBuckets.filter(bucket => {
      return bucket.type == 'user'
    })
    const bucketsCount = buckets.length

    if (bucketsCount >= bucketsMax) {
      dispatch(setBucketLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setBucketLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const checkTaskLimits = () => (dispatch, getState: GetState) => {
  try {
    const {
      cloud: {limits},
      resources,
    } = getState()
    const tasksMax = extractTaskMax(limits)
    const tasksCount = resources.tasks.allIDs.length

    if (tasksCount >= tasksMax) {
      dispatch(setTaskLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setTaskLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const checkChecksLimits = () => (dispatch, getState: GetState) => {
  try {
    const {
      resources,
      cloud: {limits},
    } = getState()

    const checksMax = extractChecksMax(limits)
    const checksCount = resources.checks.allIDs.length
    if (checksCount >= checksMax) {
      dispatch(setChecksLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setChecksLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const checkRulesLimits = () => (dispatch, getState: GetState) => {
  try {
    const {
      resources,
      cloud: {limits},
    } = getState()

    const rulesMax = extractRulesMax(limits)
    const rulesCount = resources.rules.allIDs.length

    if (rulesCount >= rulesMax) {
      dispatch(setRulesLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setRulesLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}

export const checkEndpointsLimits = () => (dispatch, getState: GetState) => {
  try {
    const state = getState()
    const endpointsCount = state.resources.endpoints.allIDs.length
    const endpointsMax = extractEndpointsMax(state.cloud.limits)

    if (endpointsCount >= endpointsMax) {
      dispatch(setEndpointsLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setEndpointsLimitStatus(LimitStatus.OK))
    }
  } catch (error) {
    console.error(error)
  }
}
