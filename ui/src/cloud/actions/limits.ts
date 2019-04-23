// API
import {getReadWriteLimitsAJAX, getLimitsAJAX} from 'src/cloud/apis/limits'

// Types
import {AppState} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {
  readWriteLimitReached,
  resourceLimitReached,
} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from '@influxdata/clockface'

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
}

export type Actions = SetLimits | SetLimitsStatus | SetDashboardLimitStatus

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

    const isReadLimited = limits.read.status === LimitStatus.EXCEEDED
    const isWriteLimited = limits.write.status === LimitStatus.EXCEEDED

    if (isReadLimited || isWriteLimited) {
      dispatch(notify(readWriteLimitReached(isReadLimited, isWriteLimited)))
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

export const checkDashboardLimits = () => async (
  dispatch,
  getState: () => AppState
) => {
  try {
    const {
      dashboards: {list},
      cloud: {
        limits: {
          dashboards: {maxAllowed},
        },
      },
    } = getState()

    const dashboardsCount = list.length

    if (maxAllowed <= dashboardsCount) {
      dispatch(setDashboardLimitStatus(LimitStatus.EXCEEDED))
      dispatch(notify(resourceLimitReached('dashboards')))
    } else {
      dispatch(setDashboardLimitStatus(LimitStatus.OK))
    }
  } catch (e) {
    console.error(e)
  }
}
