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
import {AppState, Limits, RemoteDataState} from 'src/types'
import {
  extractDashboardMax,
  extractBucketMax,
  extractTaskMax,
  extractChecksMax,
  extractRulesMax,
  extractEndpointsMax,
} from 'src/cloud/utils/limits'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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
  getState: () => AppState
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
  } catch (e) {}
}

export const getAssetLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  dispatch(setLimitsStatus(RemoteDataState.Loading))
  try {
    const org = getOrg(getState())

    const limits = await getLimitsAJAX(org.id)
    dispatch(setLimits(limits))
    dispatch(setLimitsStatus(RemoteDataState.Done))
  } catch (e) {
    dispatch(setLimitsStatus(RemoteDataState.Error))
  }
}

export const checkDashboardLimits = () => (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      dashboards: {list},
      cloud: {limits},
    } = getState()

    const dashboardsMax = extractDashboardMax(limits)
    const dashboardsCount = list.length

    if (dashboardsCount >= dashboardsMax) {
      dispatch(setDashboardLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setDashboardLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkBucketLimits = () => (dispatch, getState: () => AppState) => {
  try {
    const {
      buckets: {list},
      cloud: {limits},
    } = getState()

    const bucketsMax = extractBucketMax(limits)
    const buckets = list.filter(bucket => {
      return bucket.type == 'user'
    })
    const bucketsCount = buckets.length

    if (bucketsCount >= bucketsMax) {
      dispatch(setBucketLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setBucketLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkTaskLimits = () => (dispatch, getState: () => AppState) => {
  try {
    const {
      tasks: {list},
      cloud: {limits},
    } = getState()

    const tasksMax = extractTaskMax(limits)
    const tasksCount = list.length

    if (tasksCount >= tasksMax) {
      dispatch(setTaskLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setTaskLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkChecksLimits = () => (dispatch, getState: () => AppState) => {
  try {
    const {
      checks: {list: checksList},
      cloud: {limits},
    } = getState()

    const checksMax = extractChecksMax(limits)
    const checksCount = checksList.length
    if (checksCount >= checksMax) {
      dispatch(setChecksLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setChecksLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkRulesLimits = () => (dispatch, getState: () => AppState) => {
  try {
    const {
      rules: {list: rulesList},
      cloud: {limits},
    } = getState()

    const rulesMax = extractRulesMax(limits)
    const rulesCount = rulesList.length

    if (rulesCount >= rulesMax) {
      dispatch(setRulesLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setRulesLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkEndpointsLimits = () => (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      endpoints: {list: endpointsList},
      cloud: {limits},
    } = getState()

    const endpointsMax = extractEndpointsMax(limits)
    const endpoinstCount = endpointsList.length

    if (endpoinstCount >= endpointsMax) {
      dispatch(setEndpointsLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setEndpointsLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}
