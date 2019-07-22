// API
import {
  getReadWriteLimits as getReadWriteLimitsAJAX,
  getLimits as getLimitsAJAX,
} from 'src/cloud/apis/limits'

// Types
import {AppState} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {readLimitReached} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from '@influxdata/clockface'
import {
  extractDashboardMax,
  extractBucketMax,
  extractTaskMax,
} from 'src/cloud/utils/limits'

export enum LimitStatus {
  OK = 'ok',
  EXCEEDED = 'exceeded',
}

interface Limits {
  rate: {
    readKBs: number
    concurrentReadRequests: number
    writeKBs: number
    concurrentWriteRequests: number
  }
  bucket: {
    maxBuckets: number
  }
  task: {
    maxTasks: number
  }
  dashboard: {
    maxDashboards: number
  }
}

export enum ActionTypes {
  SetLimits = 'SET_LIMITS',
  SetLimitsStatus = 'SET_LIMITS_STATUS',
  SetDashboardLimitStatus = 'SET_DASHBOARD_LIMIT_STATUS',
  SetBucketLimitStatus = 'SET_BUCKET_LIMIT_STATUS',
  SetTaskLimitStatus = 'SET_TASK_LIMIT_STATUS',
  SetReadRateLimitStatus = 'SET_READ_RATE_LIMIT_STATUS',
  SetWriteRateLimitStatus = 'SET_WRITE_RATE_LIMIT_STATUS',
}

export type Actions =
  | SetLimits
  | SetLimitsStatus
  | SetDashboardLimitStatus
  | SetBucketLimitStatus
  | SetTaskLimitStatus
  | SetReadRateLimitStatus
  | SetWriteRateLimitStatus

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

export const getReadWriteLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    const limits = await getReadWriteLimitsAJAX(org.id)

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
  } catch (e) {}
}

export const getAssetLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  dispatch(setLimitsStatus(RemoteDataState.Loading))
  try {
    const {
      orgs: {org},
    } = getState()

    const limits = (await getLimitsAJAX(org.id)) as Limits
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

export const checkBucketLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      buckets: {list},
      cloud: {limits},
    } = getState()

    const bucketsMax = extractBucketMax(limits)
    const bucketsCount = list.length

    if (bucketsCount >= bucketsMax) {
      dispatch(setBucketLimitStatus(LimitStatus.EXCEEDED))
    } else {
      dispatch(setBucketLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}

export const checkTaskLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
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
