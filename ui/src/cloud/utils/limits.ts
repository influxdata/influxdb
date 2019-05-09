import {get} from 'lodash'
import {ASSET_LIMIT_ERROR_STATUS} from 'src/cloud/constants/index'
import {LimitsState} from 'src/cloud/reducers/limits'
import {LimitStatus} from 'src/cloud/actions/limits'

export const isLimitError = (error): boolean => {
  return get(error, 'response.status', '') === ASSET_LIMIT_ERROR_STATUS
}

export const extractBucketLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'buckets.limitStatus')
}

export const extractBucketMax = (limits: LimitsState): number => {
  return get(limits, 'buckets.maxAllowed', Infinity)
}

export const extractDashboardLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'dashboards.limitStatus')
}

export const extractDashboardMax = (limits: LimitsState): number => {
  return get(limits, 'dashboards.maxAllowed', Infinity)
}

export const extractTaskLimits = (limits: LimitsState): LimitStatus => {
  return get(limits, 'tasks.limitStatus')
}

export const extractTaskMax = (limits: LimitsState): number => {
  return get(limits, 'tasks.maxAllowed', Infinity)
}
